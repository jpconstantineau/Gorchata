
## Roadmap

### Completed Examples ✅

- [x] Star Schema
- [x] DCS Alarm Analytics
- [x] Manufacturing Data for Bottleneck Analysis
- [x] [Unit Train Analytics Data Warehouse](examples/unit_train_analytics/README.md)

### In Progress 🚧

- [x] [Haul Truck Data Transformation and Analysis](examples/haul_truck_analytics/README.md) - *Phase 5 of 8 ✅*

### Future Examples 🔮

- [] Machine Event Data to OEE
- [] Oil Refinery Data Transformation and Warehousing

## Example Design Prompts

### DCS Alarm Analytics

Act as a data engineer and analytics expert to design a data warehouse schema and analysis pipeline for refinery DCS alarm and event streams. Create a structured example featuring raw event logs—including timestamps, tag identifiers, priorities, and states (Active, Acknowledged, Inactive)—and provide the SQL logic required to transform this time-series data into actionable ISA 18.2 performance metrics. The example should specifically demonstrate how to calculate operator loading (alarms per hour), determine the duration of standing alarms, identify chattering and "bad actor" tags, and generate a summary report for overall alarm system health.

### Machine Event Data to OEE

Develop a technical data warehousing tutorial focused on OEE reporting by generating a synthetic dataset that includes: raw machine event logs (MachineID, Timestamp, State, ReasonCodeID), a reason code dimension mapping to the OEE Time Model (Planned vs. Unplanned), and production metrics (Total/Defective units) alongside machine metadata (Ideal Cycle Time). The guide should demonstrate how to calculate interval durations from discrete event timestamps using SQL window functions, classify downtime categories according to the time model, and aggregate the results into daily reporting metrics to calculate OEE (\(\text{Availability} \times \text{Performance} \times \text{Quality}\)).

### Manufacturing Data for Bottleneck Analysis 

Generate a comprehensive synthetic dataset and a star-schema data warehouse architecture for a manufacturing facility modeled after the "UniCo" plant in Eliyahu Goldratt’s The Goal, specifically focusing on tracking throughput, inventory, and operating expenses. The dataset should include high-fidelity logs for critical resources like the NCX-10 and Heat Treat departments, capturing work order timestamps, machine cycle times, scheduled/unscheduled downtime, and work-in-process (WIP) levels across various production stations. Provide the SQL-based transformation logic to calculate metrics such as queue time versus processing time and resource utilization percentages. This architecture must enable a diagnostic analysis that identifies the plant's bottlenecks by visualizing where WIP accumulation occurs and which specific resources are constraining the overall flow of the production system.

### Haul Truck Data Transformation and Analysis

Create a modular data transformation project that converts raw heavy-vehicle telemetry into structured haul cycle analytics. The project should be organized into logical layers that stage the data, identify operational states based on payload thresholds, and aggregate metrics for trip efficiency and productivity, ensuring that the business logic is well-documented and the transformation steps are clearly sequenced from source to final reporting.

### Unit Train Analytics Data Warehouse

Develop a data warehouse project that transforms raw Car Location Message (CLM) data into a structured analytical framework for railroad operations. The solution should model the full lifecycle of unit trains to analyze transit speeds and cycle times, while specifically identifying and tracking "stragglers"—cars that have been detached from their original blocks—to quantify their impact on overall delivery performance. Furthermore, the architecture must support train-to-train power transfer analysis to evaluate the efficiency of locomotive swaps and motive power utilization. The resulting environment should transition raw event logs into high-performance analytical marts optimized for reporting on rail network health and operational KPIs.


### Oil Refinery Data Transformation and Warehousing
Create a data engineering project centered on constructing a mass balance and performance monitoring system for an integrated oil refinery. The objective is to transform raw transactional records from inbound crude oil deliveries—including specific gravity and chemical composition—and outbound product shipments like gasoline, jet fuel, and raffinates into a reconciled analytical framework. The transformation logic must account for volumetric changes and processing losses to establish an accurate daily material balance across the facility. The final warehouse architecture should feature structured tables that aggregate yields and conversion rates by specific process units, enabling engineers to evaluate the efficiency of unit operations and identify performance deviations relative to theoretical benchmarks.