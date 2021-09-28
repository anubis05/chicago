We want to use the data published by city of Chicago to create a realtime data pipeline which will show the traffic condition 
by each road in near real time.
There must be some dashboards to view the data as well as notifications must be triggered when certain thresholds are met.

**Source of data**: https://data.cityofchicago.org/

Overall data pipleine designed and built here:

![Data-Pipeline](./supporting_assets/images/DataPipeline.png)

The applications which are consuiming the data from the chicago webservice and then pushing it to the data pipeline

![PublishApplication](./supporting_assets/images/PublishApplication.png)

Pushing the data to a pub sub and from there pushing it for further processing

![Ingest&Delivery](./supporting_assets/images/IngestAndDelivery.png)

We used Dataflow templates to push the data to BQ

![EventDeliveryDataFlow](./supporting_assets/images/EventDeliveryDataflow.png)

We used Datastudio for visualization.
We also stored a copy of the data inGCS buckets in Avro format for future processing.

![Processing&Analytics](./supporting_assets/images/Processing&Analytics.png)

Look at some of the BI tools available for BQ

![BIToolsForBQ](./supporting_assets/images/BIToolsForBQ.png)

We also needed to further process the data to trigger events in case some thresholds are met

![TrigerringEvents](./supporting_assets/images/TrigerringEvents.png)



