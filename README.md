# 🚨 RHS Demo Consumer

This demo consumer continuously polls warnings from the **RHS service** and saves them in [GeoJSON](https://geojson.org/) format.

By default, it creates a new `.json` file every **10 minutes**, containing the received warnings.  
You can view these files with tools like:

- [geojson.io](https://geojson.io/)
- [kepler.gl](https://kepler.gl/demo)

📁 **Example output:** [example/2025-04-04_21-46-34.json](./example/2025-04-04_21-46-34.json)

---

## 🔐 Required Credentials

Before using this consumer, request the following properties from [RHS support](https://www.bosch-mobility.com/de/loesungen/assistenzsysteme/connected-map-services/):


| Property | Description |
| - | - |
| client-id | Identity to authenticate to the RHS service |
| client-secret | Secret corresponding to the client ID|
| tenant-id | Azure tenant ID associated with the identity |
| eventhubs-namespace | URL for the Kafka client to consume from |
| group | Consumer group ID (supports multiple consumers with partitioned data) |
| topic | The topic from which warnings are polled/consumed |


## Setup

1. **Set environment variables**:  
   You can either export them manually or use a `.env` file:

   ```bash
   AZURE_CLIENT_ID=<client-id>
   AZURE_CLIENT_SECRET=<client-secret>
   AZURE_TENANT_ID=<tenant-id>
   ```
   Or create a .env file and load it with:

   ```bash
   source .env
   ```
2. **Run the consumer**
   ```bash
   python consumer.py [options...] <eventhubs-namespace> <group> <topic>
   ```


