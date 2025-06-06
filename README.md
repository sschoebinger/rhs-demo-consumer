# 🚨 RHS Demo Consumer

This repository provides two demo consumers for the **RHS service**. Both continuously poll warnings and process them as [GeoJSON](https://geojson.org/) Features:

- **File Consumer**: Saves warnings to `.json` files every **10 minutes**.
- **HTTP Consumer**: Serves the latest warnings from memory via an HTTP endpoint.

You can view the GeoJSON files with tools like:

- [geojson.io](https://geojson.io/)
- [kepler.gl](https://kepler.gl/demo)

📁 **Example output:** [example/2025-04-04_21-46-34.json](./example/2025-04-04_21-46-34.json)

---

## 🔐 Required Credentials

Before using these consumers, request the following properties from [RHS support](https://www.bosch-mobility.com/de/loesungen/assistenzsysteme/connected-map-services/):

| Property | Description |
| - | - |
| client-id | Identity to authenticate to the RHS service |
| client-secret | Secret corresponding to the client ID|
| tenant-id | Azure tenant ID associated with the identity |
| eventhubs-namespace | URL for the Kafka client to consume from |
| group | Consumer group ID (supports multiple consumers with partitioned data) |
| topic | The topic from which warnings are polled/consumed |

---

## Setup

1. **Set environment variables**:  
   You can either export them manually or use a `.env` file:

   ```bash
   AZURE_CLIENT_ID=<client-id>
   AZURE_CLIENT_SECRET=<client-secret>
   AZURE_TENANT_ID=<tenant-id>
   ```
   Or create a `.env` file and load it with:

   ```bash
   source .env
   ```

2. **Run the File Consumer**  
   This will write GeoJSON files every 10 minutes:
   ```bash
   python src/consumer-file.py [options...] <eventhubs-namespace> <group> <topic>
   ```

3. **Run the HTTP Consumer**  
   This will start an HTTP server (default port 8000) that serves the latest warnings:
   ```bash
   python src/consumer-http.py [options...] <eventhubs-namespace> <group> <topic>
   ```
   - The endpoint `/` returns the current in-memory GeoJSON FeatureCollection.
   - Either set the `?api_key` parameter or the `Authorization` header to the specified token
   - The default token `changeme` can be configured by setting the `API_TOKEN` environment variable

---

## 📖 Notes

- Both consumers require the same credentials and Kafka/EventHub parameters.


