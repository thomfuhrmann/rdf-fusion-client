use a5_geozarr::query::QueryCoordsBuilder;
use anyhow::Context;
use datafusion::logical_expr::async_udf::AsyncScalarUDF;
use futures::StreamExt;
use rdf_fusion::common::RdfFormat;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::storage::rdf_files::RdfFileScanOptions;
use rdf_fusion::store::Store;
use rdf_fusion_client::scalar_udf::A5GeoZarrUdf;
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

    let query_coords = QueryCoordsBuilder::new()
        .insert_range_dt_str(
            "time",
            Some("2025-11-01T00:00:00Z"),
            Some("2025-12-01T00:00:00Z"),
        )
        .context("Could not build query coordinates")?
        .build();

    let json_coords = serde_json::to_string(&query_coords).unwrap();

    let query = format!(
        r#"
    PREFIX a5: <https://a5geo.org/>
    PREFIX hzb: <https://resource.geosphere.at/hzb/>

    SELECT ?cell (<https://geosphere.at/fetchGeoZarr>(?cell, '{}', 'RR') AS ?cellValue)
    WHERE {{
        hzb:2-370-87-25-0-0-0-0-0 a5:hasCell ?cell .
    }}
    "#,
        json_coords
    );
    let result = store.query(&query).await?;

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

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::PathBuf;

    use super::*;
    use datafusion::datasource::listing::ListingTableUrl;
    use insta::assert_snapshot;
    use rdf_fusion::common::RdfFormat;
    use rdf_fusion::execution::results::QueryResultsFormat;
    use rdf_fusion::store::Store;
    use tokio::fs::File;
    use url::Url;

    #[tokio::test]
    async fn test_custom_udf() -> anyhow::Result<()> {
        let store = Store::new_in_memory().await;
        let _context = store.context();

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

    fn resolve_location(location: &str) -> Result<String, Box<dyn std::error::Error>> {
        let stripped = location.strip_prefix("file://").unwrap_or(location);
        if location.contains("://") && !location.starts_with("file://") {
            Ok(location.to_owned())
        } else {
            let path = PathBuf::from(stripped);
            let absolute_path = if path.is_absolute() {
                path
            } else {
                env::current_dir()?.join(path)
            };
            let mut url = Url::from_file_path(absolute_path).unwrap().to_string();
            // A trailing `/` denotes a directory, which `Url::from_file_path` drops.
            // if location.ends_with('/') && !url.ends_with('/') {
            //     url.push('/');
            // }
            Ok(url)
        }
    }

    #[test]
    fn test_resolve() -> Result<(), Box<dyn std::error::Error>> {
        let location_str = resolve_location(&"file:///home/thomas/Downloads/test/")?;
        let location_url = Url::parse(&location_str)?;

        println!("{location_url}");
        Ok(())
    }
}
