pub const REGISTER_DATASET: &str = r#"
    INSERT INTO datasets (dataset_name, manager_id, dataset_compression, dataset_encoding, dataset_classification, dataset_desc) 
    VALUES ($1, $2, $3, $4, $5, $6) 
    RETURNING dataset_id, dataset_name, manager_id, dataset_compression, dataset_encoding, dataset_classification, dataset_desc, created_at, updated_at
"#;

pub const FIND_DATASET: &str = r#"
    SELECT dataset_id, dataset_name, manager_id, dataset_compression, dataset_encoding, dataset_classification, dataset_desc, created_at, updated_at
    FROM datasets
    WHERE dataset_name = $1
"#;

pub const LIST_DATASETS: &str = r#"
    SELECT dataset_id, dataset_name, manager_id, dataset_compression, dataset_encoding, dataset_classification, dataset_desc, created_at, updated_at
    FROM datasets
    ORDER BY created_at ASC
"#;

pub const REGISTER_PARTITION: &str = r#"
    INSERT INTO partitions (partition_name, partition_url, dataset_id)
    VALUES ($1, $2, $3)
    RETURNING partition_id, partition_name, partition_url, dataset_id, created_at, updated_at
"#;

pub const FIND_PARTITION: &str = r#"
    SELECT partition_id, partition_name, partition_url, dataset_id, created_at, updated_at
    FROM partitions 
    WHERE partition_name = $1 AND dataset_id = $2
"#;

pub const FIND_PARTITION_LATEST: &str = r#"
    SELECT partition_id, partition_name, partition_url, dataset_id, created_at, updated_at
    FROM partitions 
    WHERE dataset_id = $1
    ORDER BY created_at DESC
    LIMIT 1
"#;

pub const LIST_PARTITIONS: &str = r#"
    SELECT partition_id, partition_name, partition_url, dataset_id, created_at, updated_at
    FROM partitions 
    WHERE dataset_id = $1
"#;

pub const FIND_MANAGER: &str = r#"
    SELECT manager_id, manager_email, manager_hash, manager_salt, api_key, is_admin, created_at, updated_at
    FROM managers
    WHERE api_key = $1
"#;

pub const MANAGED_DATASETS: &str = r#"
    SELECT dataset_id, dataset_name, datasets.manager_id, dataset_compression, dataset_encoding, dataset_classification, dataset_desc, datasets.created_at, datasets.updated_at
    FROM datasets
    JOIN managers ON managers.manager_id = datasets.manager_id
    WHERE managers.api_key = $1
"#;

pub const REGISTER_MANAGER: &str = r#"
    INSERT INTO managers (manager_email, manager_hash, manager_salt, api_key)
    VALUES ($1, $2, $3, $4)
    RETURNING manager_id, manager_email, api_key, manager_hash, manager_salt, is_admin, created_at, updated_at
"#;

pub const AUTH_MANAGER: &str = r#"
    SELECT manager_id, manager_email, api_key, manager_hash, manager_salt, is_admin, created_at, updated_at
    FROM managers
    WHERE manager_email = $1
"#;
