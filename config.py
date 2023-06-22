configuration = {
  "CLOUDKARAFKA_BROKERS":"dory.srvs.cloudkafka.com:9094",
  "CLOUDKARAFKA_HOSTNAME":"dory.srvs.cloudkafka.com",
  "CLOUDKARAFKA_USERNAME":"uqgxcxnv",
  "CLOUDKARAFKA_PASSWORD":"8f5UhLTnfPIm-yGkCHT13B3jjLR-Ggki",
  "CLOUDKARAFKA_TOPIC":"uqgxcxnv-messageAlert",
  "SESSION_TIMEOUT": 10000,
  "SECURITY_PROTOCOL": "SASL_SSL",
  "SASL_MECHANISM": "SCRAM-SHA-256"
}

FIREBASE_CONF = {
  "FIREBASE_CREDENTIALS":'security/distributed-system.json',
  "FIREBASE_STORAGE_BUCKET":'distributed-system-347306.appspot.com'
}

FIRESTORE_PARAMS = {
  #"CLOUD_ID":"violence_detection:dXMtZWFzdC0yLmF3cy5lbGFzdGljLWNsb3VkLmNvbTo0NDMkNDM0NTZkNDhhYTNhNDU5NGE1YmE2YmJjMTE2MDljMDckNjk0ZjcyODg5MzAzNDRkNzgzOTdhYTk1ZWFkOGFmMTA=",
  #"ELASTIC_PASSWORD":"pX7J6iNRA5XFAYYOQwTozRhk",
  #"ELASTIC_USERNAME":"elastic",
  "INDEX_NAME": 'violence-data'
}