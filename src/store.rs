use std::sync::Arc;

use datafusion::{execution::object_store::DefaultObjectStoreRegistry, object_store::ObjectStore};
use rdf_fusion::store::Store;

pub fn create_store() -> Store {
    let memory_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let registry = DefaultObjectStoreRegistry::new();
    registry.register_store(
        &Url::parse("memory:///").expect("valid object store url"),
        Arc::clone(&memory_store),
    );

    let runtime_env = RuntimeEnvBuilder::default()
        .with_object_store_registry(Arc::new(registry))
        .build_arc()
        .expect("Valid configuration");

    let url = Url::parse("memory:///delta/").unwrap();

    let log_store = logstore_with(
        memory_store,
        &url,
        StorageConfig::default().with_io_runtime(IORuntime::RT(Handle::current())),
    )
    .expect("Valid log store");

    let delta_storage = DeltaQuadsStorageBuilder::new()
        .with_log_store(log_store)
        .build()
        .await
        .expect("Default in-memory works");

    let context = rdf_fusion_execution::RdfFusionContextBuilder::new(storage)
        .with_single_partition_session_config()
        .build()
        .expect("The default configuration is valid");

    Store::new(context)
}

pub fn load_ttl_file() -> Result<(), Box<dyn Error>> {
    let file = tokio::fs::File::open("./data/a5_15_hzb_fid_13987_uncompact.ttl")
        .await
        .context("Could not find Turtle file")?;
    store
        .load_from_reader(file, RdfFileScanOptions::with_format(RdfFormat::Turtle))
        .await?;
    Ok(())
}
