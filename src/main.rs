// pub mod aggregate;

use a5_geozarr::client::{GeoZarrClient, QueryCoordsBuilder, StoreType};
use anyhow::Context;
use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::{AsArray, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use rdf_fusion::common::{DFResult, NamedNode, RdfFormat};
use rdf_fusion::encoding::typed_family::{
    DowncastTypedFamilyArray, NumericFamilyArray, TypedFamilyEncoding,
};
use rdf_fusion::encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingDatum, EncodingName, RdfFusionEncodings,
    TermEncoding, detect_encoding_from_types,
};
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::extensions::functions::FunctionName;
use rdf_fusion::functions::scalar::args::ScalarSparqlFunctionArgs;
use rdf_fusion::functions::scalar::signature::SparqlUDFTypeSignatureBuilder;
use rdf_fusion::storage::rdf_files::RdfFileScanOptions;
use rdf_fusion::store::Store;
use std::fmt::{Debug, Formatter};
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

pub(crate) fn get_path(relative: &str) -> PathBuf {
    let fixture_path = Path::new(relative).canonicalize().expect("should work");
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_path.join(fixture_path)
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let store = Store::new_in_memory().await;
    let file = tokio::fs::File::open("./data/a5_15_hzb_fid_13987_uncompact.ttl")
        .await
        .context("Could not find Turtle file")?;
    store
        .load_from_reader(file, RdfFileScanOptions::with_format(RdfFormat::Turtle))
        .await?;

    let context = store.context();
    context.functions().register_udf(Arc::new(
        AsyncScalarUDF::new(Arc::new(A5GeoZarrUdf::new(context.encodings().clone())))
            .into_scalar_udf(),
    ));

    // TODO: maybe use "2026-10-01T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> instead of string?
    let query = "
    PREFIX a5: <https://a5geo.org/>
    PREFIX hzb: <https://resource.geosphere.at/hzb/>

    SELECT ?cell (<https://geosphere.at/fetchGeoZarr>(?cell, '2026-10-01T00:00:00Z', 'TM') AS ?cellValue)
    WHERE {
        hzb:2-370-87-25-0-0-0-0-0 a5:hasCell ?cell .
    }
    ";
    let result = store.query(query).await?;

    if let QueryResults::Solutions(mut stream) = result {
        while let Some(solution) = stream.next().await {
            if let Ok(solution) = solution {
                let values = solution.values();
                println!("{values:?}");
            } else {
                println!("no solution");
            }
        }
    }

    Ok(())
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

                let datum = args.args()[1].clone();
                let date_time = unwrap_string(datum, args.number_rows());
                let Some(date_time) = date_time else {
                    return Err(DataFusionError::Execution(
                        "Invalid datetime argument".into(),
                    ));
                };

                let datum = args.args()[2].clone();
                let data_var = unwrap_string(datum, args.number_rows());
                let Some(data_var) = data_var else {
                    return Err(DataFusionError::Execution(
                        "Invalid data variable argument".into(),
                    ));
                };

                // create object store URL
                let path = get_path("data/spartacus.icechunk.zarr");
                let url = Url::from_file_path(path).map_err(|_| {
                    DataFusionError::Execution("Could not parse object store URL".into())
                })?;

                // create virtual chunk URL
                let virtual_chunks_url = Url::parse("https://public.hub.geosphere.at/")
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // parse query coordinates
                let query_coords = QueryCoordsBuilder::new()
                    .temporal_dim_str("time", date_time)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .other_dim("z", 0)
                    .build();

                let data = self
                    .geozarr_client
                    .fetch_data(
                        &url,
                        &cell_ids,
                        &data_var,
                        &query_coords,
                        StoreType::Icechunk,
                        Some(&virtual_chunks_url),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let tf_arr = self.encodings.typed_family().create_array_from_family(
                    NumericFamilyArray::new_ints(Int32Array::from(data)),
                )?;

                Ok(ColumnarValue::Array(tf_arr.into_array_ref()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_snapshot;
    use rdf_fusion::common::RdfFormat;
    use rdf_fusion::execution::results::QueryResultsFormat;
    use rdf_fusion::store::Store;
    use tokio::fs::File;

    #[tokio::test]
    async fn test_custom_udf() -> anyhow::Result<()> {
        let store = Store::new_in_memory().await;
        let context = store.context();

        // let udf = ContainsSpiderUDF::new(context.encodings().clone());
        // context
        //     .functions()
        //     .register_udf(Arc::new(ScalarUDF::new_from_impl(udf)))?;

        let file_path = "./data/a5_15_hzb_fid_13987_uncompact.ttl";
        let file = File::open(file_path)
            .await
            .with_context(|| format!("Failed to open test data at {}", file_path))?;

        store
            .load_from_reader(file, RdfFileScanOptions::with_format(RdfFormat::Turtle))
            .await?;

        let query = "
            BASE <https://geosphere.at/>
            PREFIX rel: <http://www.perceive.net/schemas/relationship/>

            SELECT ?subject ?predicate ?object
            WHERE {
                ?subject ?predicate ?object .
                FILTER(<https://geosphere.at/containsSpider>(?object))
            }
        ";

        let result_set = store.query(query).await?;
        let mut buffer = Vec::new();
        result_set
            .write(&mut buffer, QueryResultsFormat::Tsv)
            .await?;
        let output = String::from_utf8(buffer)?;

        assert_snapshot!(output, @r#"
        ?subject	?predicate	?object
        <https://geosphere.at/#spiderman>	<http://xmlns.com/foaf/0.1/name>	"Spiderman"
        "#);

        Ok(())
    }
}
