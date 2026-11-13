use anyhow::Context;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use futures::StreamExt;
use rdf_fusion::common::{DFResult, NamedNode, RdfFormat};
use rdf_fusion::encoding::typed_family::{
    BooleanFamilyArray, DowncastTypedFamilyArray, StringFamilyArray,
};
use rdf_fusion::encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::extensions::functions::FunctionName;
use rdf_fusion::functions::scalar::args::ScalarSparqlFunctionArgs;
use rdf_fusion::functions::scalar::signature::SparqlUDFTypeSignatureBuilder;
use rdf_fusion::storage::rdf_files::RdfFileScanOptions;
use rdf_fusion::store::Store;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

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

    let query = "
    SELECT ?subject (<https://geosphere.at/fetchGeoZarr>(?object) AS ?fetch)
    WHERE {
        ?subject ?predicate ?object .
    }
    LIMIT 1
    ";
    let result = store.query(query).await?;

    match result {
        QueryResults::Solutions(mut stream) => {
            while let Some(solution) = stream.next().await {
                if let Ok(solution) = solution {
                    let values = solution.values();
                    println!("{values:?}");
                } else {
                    println!("no solution");
                }
            }
        }
        _ => {}
    }

    Ok(())
}

/// Checks whether a given *literal* contains the string `spider` (case-insensitive).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct A5GeoZarrUdf {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl A5GeoZarrUdf {
    /// Creates a new [ContainsSpiderUDF].
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlUDFTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            // How to call the function in SPARQL.
            name: FunctionName::Custom(NamedNode::new_unchecked(
                "https://geosphere.at/fetchGeoZarr",
            ))
            .to_string(),
            // The signature of the function. For an explanation of Volatility, see the DataFusion
            // documentation. This basically means that the function always maps the same input to
            // the same output (as opposed to, for example, rand())
            signature: Signature::new(type_signature, Volatility::Immutable),
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
            _ => exec_err!("Fetch data UDF only supports the TypedFamily encoding"),
        }
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        not_impl_err!("AsyncUpper can only be called from async contexts")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for A5GeoZarrUdf {
    fn ideal_batch_size(&self) -> Option<usize> {
        // evaluate the whole batch
        None
    }

    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let body = reqwest::get("https://www.rust-lang.org")
            .await
            .map_err(|e| DataFusionError::External(e.into()))?
            .text()
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(array)) => {
                let result = array.map_children_tf_unary(|child| {
                    match child.as_downcast_array() {
                        // If the input is null or a resource, return null. The output is a
                        // TypedFamilyEncoding array with nulls.
                        DowncastTypedFamilyArray::Null(_) => self
                            .encodings
                            .typed_family()
                            .create_null_array(child.to_array().len()),
                        DowncastTypedFamilyArray::Iri(array) => {
                            let num_rows = array.inner().len();
                            let result = StringArray::from(vec![Some(body.clone()); num_rows]);
                            self.encodings
                                .typed_family()
                                .create_array_from_family(StringFamilyArray::new_simple(result))
                        }
                        DowncastTypedFamilyArray::BlankNode(_) => self
                            .encodings
                            .typed_family()
                            .create_null_array(child.to_array().len()),
                        // If the input is a string, check whether it contains the string. The
                        // output is a TypedFamilyEncoding array with booleans.
                        DowncastTypedFamilyArray::String(array) => {
                            let num_rows = array.inner().len();
                            let result = StringArray::from(vec![Some("test"); num_rows]);
                            self.encodings
                                .typed_family()
                                .create_array_from_family(StringFamilyArray::new_simple(result))
                        }
                        // For all other families, return false.
                        _ => {
                            let result = BooleanArray::from(vec![false; child.to_array().len()]);
                            self.encodings
                                .typed_family()
                                .create_array_from_family(BooleanFamilyArray::new(result))
                        }
                    }
                })?;

                Ok(ColumnarValue::Array(result.into_array_ref()))
            }
            _ => exec_err!("Fetch data UDF was called with an unsupported encoding."),
        }
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
