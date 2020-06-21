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
- `DD_CALLBACK_HOST`: HTTP host of the Data Dictionary service in your environment (e.g. `"https://datadict.svc.local"`)