# Data Dictionary

### About

## Usage

### API

### Environment Variables

- `DD_MANAGER_EMAIL_DOMAIN`: optional, used to validate manager email address is from certain domain (e.g. recurly.com)
- `DD_DATABASE_PARAMS`: database connection information (e.g. `"host=127.0.0.1 user=postgres port=5432"`)
- `DD_SUBSCRIPTION_NAME`: Pubsub subscription name created for notifying Data Dictionary of bucket events
- `DD_GCP_PROJECT_ID`: Google Cloud Project ID associated with the environment 
- `DD_TOPIC_NAME`: Pubsub topic name created for bucket event message transfer
- `DD_BUCKET_NAME_PRIVATE`: Bucket name for the datasets containing data with "private" classification
- `DD_BUCKET_NAME_PUBLIC`: Bucket name for the datasets containing data with "public" classification
- `DD_BUCKET_NAME_SENSITIVE`: Bucket name for the datasets containing data with "sensitive" classification
- `DD_BUCKET_NAME_CONFIDENTIAL`: Bucket name for the datasets containing data with "confidential" classification
- `DD_PUBSUB_SERVICE`: URL of the global or region-specific Pub/Sub service (e.g. `"https://pubsub.googleapis.com"`)
- `DD_STORAGE_SERVICE`: URL of the Cloud Storage service (e.g. `"https://storage.googleapis.com"`)
- `GOOGLE_APPLICATION_CREDENTIALS`: optional, path to the service account key on disk (e.g. `"path/to/key.json"`)