# RHS demo consumer

This demo consumer polls the warnings from the RHS service continuously. By default it creates each 10 minutes a json file which contains the received warnings.

The following properties must be requested from the [RHS support](https://www.bosch-mobility.com/de/loesungen/assistenzsysteme/connected-map-services/):

| Property | Description |
| - | - |
| client-id | the identity to authenticate to the RHS service |
| client-secret | the corresponding secret |
| tenant-id | an ID to let azure know which identity belongs to |
| eventhubs-namespace | the URL to let the kafka client know from where to consume |
| group | you can consume with multiple consumers from this group and the data will be partitioned between them |
| topic | the topic from which the warnings are polled/consumed |


## Setup

In order to run the consumer, set the `AZURE_CLIENT_ID=<client-id>`, `AZURE_CLIENT_SECRET=<client-secret>` and `AZURE_TENANT_ID=<tenant-id>` environment variables (e.g. by using the `.env` file and load it by running `source .env` in the folder where the `.env` file is located).

Then execute `consumer.py [options..] <eventhubs-namespace> <group> <topic>`.

