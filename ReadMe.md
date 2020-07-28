# ABAP_Kafka_Producer
**Background**

Historically organizations have used EIS / DSS / BW type solutions to offload reporting and analytics from their transactional systems.

In many cases these solutions have become data federation layers, with embedded mapping and translation.

Where the solutions are used for data federation a tight coupling to the data structure has caused duplication of data for similar needs and a lack of flexibility to change.

In most scenarios data loaded in batch, and the process of orchestrating the loads is error prone.



**Vision**

The vision of this project is develop producers and consumers for common ERP applications including SAP, JDE etc. These examples will then be included in Confluents examples project and hopefully adopted by many ERP customers.

As of the date of writing important PAAS capabilities exist to address our current state needs without capital investment, and with almost unlimited compute and storage options.

These solutions include – Kafka event streaming and Google Big Query, which together with Avro schema allow the current functions of on-premise EIS/DSS/BW systems to be migrated to cloud PAAS whilst enabling some system improvements.

Kafka (Confluent Cloud) provides; 
	At most once, At least once, Exactly once processing gurantees
	Retention, with tiered storage upto unlimited
	Replication across clusters
	Significant application and REST API support
	KSQL and stream processing for aggregate topic definition
	Avro schema support
	Massive scalability
	SINK connectors for rapid federation 
	
Big Query (GCP) provides;
	Massive scaling for query execution
	Avro schema support
	Many data sources, including tables, storage etc.

With these primary components, most of the data federation, query and many data API’s can be offloaded to Confluent Cloud and GCP. 

Together these solutions would support many objectives;
	Implement a stranger pattern on legacy monolithic applications, i.e ERP.
	Provide API’s for data consumers, i.e. cloud native applications 
	Remove the dependency on tightly coupled data structures.
	Create re-usable topics vs point to point ETL
	Support unlimited (or record retention policy compliant) data retention
	Federate data to other solutions, i.e. Snowflake, Elastic Search etc.
	Support Cloud migrations
	
**About this Project**

Kafka ingests data, to topics,  from producers. Topics can be used in a variety of ways. Principally Topics are consumed. Consumers can read with an offset, theoretically reloading data from the first available event.  Topics can be used with stream processing, KSQL and SINK connectors too.  In this model a produce once, consume many approach helps reduce the number of point to point integrations needed in an organization.

This project focuses on a specific producer / consumer for SAP ABAP. Many examples of producer / consumers exist in many languages but there are no fully developed open source producers or consumers for ABAP.

In general producers fall into three categories.

	1 – Direct application producers.  These formulate the topic contents, interact with the Confluent Cloud cluster directly over a network connection without any intermediate connector or component. The application logic controls what data is produced, and how to handle any error situation.

	2 – JDBC connector, is an intermediate component which polls a db with a SQL query and publishes the result to a specific topic.

	3 – Change Data Capture, is an approach where DB redo logs are interpreted and the topic mimics the db changes at a table level.  Examples of tools in this space include Qlik replicate and IBM CDC tool.

	4 – Confluent connectors.  These include ‘replicate’ and GCP Pub Sub etc.

This project falls into category 1.

>Development environment.

For development the Confluent environment can be deployed as a docker container usinig the quickstart – https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html#

Alternatively, a ‘free’ confluent cloud account can be created.  There are some features, like authentication which are enabled in confluent cloud but not the quickstart.

SAP environment is the SAP NetWeaver AS ABAP 7.51 SP02 on ASE this environment can be deployed through CAL.SAP.COM to your preferred cloud scale provider, in my case GCP.

There is no JDE development environment know at this time.

Other useful links
	https://github.com/confluentinc/examples




Current State and prioritized stories.


	
SAP ABAP Producer example
This code was found on an SAP forum and accredited to an unknown former member. The code has been tested and works in current state. The example uses ABAP channels, which allows bi-directional socket connections to be established with clients.  This is only available in Netweaver 7.4 (check)


The following Issues have been created for this code;
1 – Add authentication for confluent cloud -SASL/TLS PLAIN authentication.  
2 – Add Avro schema support / combine with SAP ABAP data dictionary?

SAP ABAP Consumer example
1 -Not started. Start

IBM JD Edwards
1-	Not started

## Donations 

Please consider supporting this and future yoklii projects by becoming a patreon at the link below.

<a href="https://www.patreon.com/bePatron?u=4167417" data-patreon-widget-type="become-patron-button">Become a Patron!</a><script async src="https://c6.patreon.com/becomePatronButton.bundle.js"></script>


## License

![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)
**[GPLv3 license]( https://www.gnu.org/licenses/gpl-3.0)**

Copyright 2020 © <a href="https://yoklii.com" target="_blank">yoklii.com</a>.




