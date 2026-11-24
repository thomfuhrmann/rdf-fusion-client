use a5_geozarr::client::{GeoZarrClient, SampledData, StoreType};
use a5_geozarr::query::QuerySelection;
use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::async_udf::AsyncScalarUDFImpl;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use rdf_fusion::common::{DFResult, NamedNode};
use rdf_fusion::encoding::typed_family::{DowncastTypedFamilyArray, TypedFamilyEncoding};
use rdf_fusion::encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingDatum, EncodingName, RdfFusionEncodings,
    TermEncoding, detect_encoding_from_types,
};
use rdf_fusion::extensions::functions::FunctionName;
use rdf_fusion::functions::scalar::args::ScalarSparqlFunctionArgs;
use rdf_fusion::functions::scalar::signature::SparqlUDFTypeSignatureBuilder;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

use crate::tensor::RdfLiteral;

pub(crate) fn get_path(relative: &str) -> PathBuf {
    let fixture_path = Path::new(relative).canonicalize().expect("should work");
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_path.join(fixture_path)
}

#[derive(Clone)]
pub struct A5GeoZarrUdf {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
    geozarr_client: Arc<GeoZarrClient>,
}

impl std::hash::Hash for A5GeoZarrUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for A5GeoZarrUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for A5GeoZarrUdf {}

impl A5GeoZarrUdf {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlUDFTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_ternary_arity()
            .build();
        Self {
            encodings,
            name: FunctionName::Custom(NamedNode::new_unchecked(
                "https://geosphere.at/fetchGeoZarr",
            ))
            .to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
            geozarr_client: Arc::new(GeoZarrClient::new()),
        }
    }
}

impl Debug for A5GeoZarrUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("A5GeoZarrUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl ScalarUDFImpl for A5GeoZarrUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match detect_encoding_from_types(&self.encodings, arg_types)? {
            Some(EncodingName::TypedFamily) => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("A5GeoZarrUdf only supports TypedFamily encoding"),
        }
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        not_impl_err!("A5GeoZarrUdf can only be called within async contexts")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for A5GeoZarrUdf {
    fn ideal_batch_size(&self) -> Option<usize> {
        // evaluate the whole batch
        None
    }

    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 3 {
            return Err(DataFusionError::Execution(
                "Invalid number of arguments".into(),
            ));
        }

        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(args)) => {
                // parse cell ids
                let array = args.args()[0].clone();
                let cell_ids =
                    if let Some(child) = array.try_get_homogeneous_child(args.number_rows()) {
                        match child.as_downcast_array() {
                            DowncastTypedFamilyArray::Iri(array) => array
                                .inner()
                                .iter()
                                .flatten()
                                .filter_map(|s| cell_id_from_url(s).ok())
                                .collect::<Vec<_>>(),
                            _ => Vec::new(),
                        }
                    } else {
                        Vec::new()
                    };

                // parse query coordinates
                let datum = args.args()[1].clone();
                let datum_str = unwrap_string(datum, args.number_rows());
                let Some(datum_str) = datum_str else {
                    return Err(DataFusionError::Execution(
                        "Invalid query coordinates".into(),
                    ));
                };
                let query_coords: HashMap<String, QuerySelection> =
                    serde_json::from_str(&datum_str).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to parse QueryCoords JSON: {}",
                            e
                        ))
                    })?;

                // parse data variable
                let datum = args.args()[2].clone();
                let data_var = unwrap_string(datum, args.number_rows());
                let Some(data_var) = data_var else {
                    return Err(DataFusionError::Execution(
                        "Invalid data variable argument".into(),
                    ));
                };

                let path = get_path("data/spartacus.icechunk.zarr");
                let url = Url::from_file_path(path).map_err(|_| {
                    DataFusionError::Execution("Could not parse object store URL".into())
                })?;

                let virtual_chunks_url = Url::parse("https://public.hub.geosphere.at/")
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let (data, coords) = self
                    .geozarr_client
                    .fetch_data(
                        &url,
                        &cell_ids,
                        &data_var,
                        query_coords,
                        StoreType::Icechunk,
                        Some(&virtual_chunks_url),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                match data {
                    SampledData::I32(data) => {
                        let literals = data
                            .into_iter()
                            .map(|sample| RdfLiteral::from_sample(sample, coords.clone()))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|e| DataFusionError::External(e))?;
                        let array = RdfLiteral::into_unknown_family_array(literals)?;
                        let tf_arr = self
                            .encodings
                            .typed_family()
                            .create_array_from_family(array)?;

                        Ok(ColumnarValue::Array(tf_arr.into_array_ref()))
                    }
                    _ => return Err(DataFusionError::Execution("Unexpected data type".into())),
                }
            }
            _ => exec_err!("A5GeoZarrUdf was called with an unsupported encoding"),
        }
    }
}

#[derive(Error, Debug)]
pub enum A5ExtractError {
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    #[error("Missing path segment in URL")]
    MissingPathSegment,

    #[error("Invalid cell ID format: {0}")]
    InvalidCell(#[from] std::num::ParseIntError),

    #[error("Invalid RDF term: {0}")]
    InvalidRdfTerm(String),
}

/// Extracts and parses an A5 64-bit cell ID from a URL path
pub fn cell_id_from_url(url_str: &str) -> Result<u64, A5ExtractError> {
    let parsed_url = Url::parse(url_str).map_err(A5ExtractError::InvalidUrl)?;

    if parsed_url.domain() != Some("a5geo.org") {
        return Err(A5ExtractError::InvalidRdfTerm(url_str.into()));
    }

    let segment = parsed_url
        .path_segments()
        .ok_or(A5ExtractError::MissingPathSegment)?
        .rfind(|s| !s.is_empty())
        .ok_or(A5ExtractError::MissingPathSegment)?;

    let cell_id = if let Some(hex_str) = segment
        .strip_prefix("0x")
        .or_else(|| segment.strip_prefix("0X"))
    {
        u64::from_str_radix(hex_str, 16)
    } else {
        u64::from_str(segment)
    }
    .map_err(A5ExtractError::InvalidCell)?;

    Ok(cell_id)
}

fn unwrap_string(datum: EncodingDatum<TypedFamilyEncoding>, num_rows: usize) -> Option<String> {
    if let Some(child) = datum.try_get_homogeneous_child(num_rows) {
        let val = child.value();
        if let ColumnarValue::Scalar(scalar) = val
            && let ScalarValue::Struct(inner) = scalar
        {
            let col = inner.column(0);
            col.as_string_opt::<i32>().and_then(|arr| {
                if arr.is_valid(0) {
                    Some(arr.value(0).to_string())
                } else {
                    None
                }
            })
        } else {
            None
        }
    } else {
        None
    }
}
