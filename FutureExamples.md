
## Roadmap

### Completed Examples ✅

- [x] Star Schema
- [x] DCS Alarm Analytics
- [x] Manufacturing Data for Bottleneck Analysis
- [x] [Unit Train Analytics Data Warehouse](examples/unit_train_analytics/README.md)
- [x] [Haul Truck Analytics Data Warehouse](examples/haul_truck_analytics/README.md) - *Complete ✅*

### In Progress 🚧

*No examples currently in progress*

### Future Examples 🔮

- [] [Machine Event Data to OEE (OSB Manufacturing)](plans/osb-machine-event-oee-plan.md) - *Planning Complete ✅ | Implementation: Not Started*
- [] Oil Refinery Data Transformation and Warehousing
- [] API 584 IOW Data Warehouse

## Example Design Prompts

### DCS Alarm Analytics

Act as a data engineer and analytics expert to design a data warehouse schema and analysis pipeline for refinery DCS alarm and event streams. Create a structured example featuring raw event logs—including timestamps, tag identifiers, priorities, and states (Active, Acknowledged, Inactive)—and provide the SQL logic required to transform this time-series data into actionable ISA 18.2 performance metrics. The example should specifically demonstrate how to calculate operator loading (alarms per hour), determine the duration of standing alarms, identify chattering and "bad actor" tags, and generate a summary report for overall alarm system health.

### Machine Event Data to OEE (OSB Manufacturing)

Design a comprehensive data engineering project for an Oriented Strand Board (OSB) manufacturing facility that transforms raw machine event logs into an integrated OEE and operational analytics platform. The project should model the complete OSB production flow: raw log handling (log pond, debarking), strand production (stranding, drying, screening), mat formation (blending, forming), board pressing (continuous hot press), and finishing operations (cooling, sawing, stacking, warehousing). Generate realistic synthetic datasets capturing discrete machine state events (Running, Idle, Starved, Blocked, Unplanned Downtime, Planned Maintenance) with reason codes mapped to both the OEE Time Model and OSB-specific failure modes (e.g., "Press Hydraulic Failure", "Dryer Burner Trip", "Strand Bridging", "Resin Mix Ratio Deviation"). 

The architecture must track production metrics including panel throughput, thickness tolerances, density specifications, and edge trim waste rates. Critically, the solution should model material flow constraints and buffer inventories between process stages (green strand bins, dry fiber silos, mat buffer) to analyze downtime propagation—demonstrating how a dryer outage cascades upstream to starve the stranders and downstream to deplete forming station buffers. 

Implement SQL transformation logic using window functions to calculate state durations, classify downtime into Availability Loss categories (breakdowns vs. setup/adjustments), compute Performance Loss from speed reductions and minor stops, and determine Quality Loss from off-spec panels requiring downgrade or scrap. Beyond standard OEE metrics, the data marts should support advanced operational KPIs including Mean Time Between Failures (MTBF), Mean Time To Repair (MTTR), utilization rates by production area, and constraint analysis identifying which process stage (stranding capacity, dryer throughput, press cycle time, or sawing speed) limits overall plant output. 

The final deliverable should enable maintenance teams to prioritize reliability improvements by identifying "bad actor" equipment with chronic breakdown patterns, and support production optimization by quantifying the economic impact of buffer sizing decisions and targeted capacity investments to eliminate the current system constraint.

### Manufacturing Data for Bottleneck Analysis 

Generate a comprehensive synthetic dataset and a star-schema data warehouse architecture for a manufacturing facility modeled after the "UniCo" plant in Eliyahu Goldratt’s The Goal, specifically focusing on tracking throughput, inventory, and operating expenses. The dataset should include high-fidelity logs for critical resources like the NCX-10 and Heat Treat departments, capturing work order timestamps, machine cycle times, scheduled/unscheduled downtime, and work-in-process (WIP) levels across various production stations. Provide the SQL-based transformation logic to calculate metrics such as queue time versus processing time and resource utilization percentages. This architecture must enable a diagnostic analysis that identifies the plant's bottlenecks by visualizing where WIP accumulation occurs and which specific resources are constraining the overall flow of the production system.

### Haul Truck Data Transformation and Analysis

Create a modular data transformation project that converts raw heavy-vehicle telemetry into structured haul cycle analytics. The project should be organized into logical layers that stage the data, identify operational states based on payload thresholds, and aggregate metrics for trip efficiency and productivity, ensuring that the business logic is well-documented and the transformation steps are clearly sequenced from source to final reporting.

### Unit Train Analytics Data Warehouse

Develop a data warehouse project that transforms raw Car Location Message (CLM) data into a structured analytical framework for railroad operations. The solution should model the full lifecycle of unit trains to analyze transit speeds and cycle times, while specifically identifying and tracking "stragglers"—cars that have been detached from their original blocks—to quantify their impact on overall delivery performance. Furthermore, the architecture must support train-to-train power transfer analysis to evaluate the efficiency of locomotive swaps and motive power utilization. The resulting environment should transition raw event logs into high-performance analytical marts optimized for reporting on rail network health and operational KPIs.


### Oil Refinery Data Transformation and Warehousing
Create a data engineering project centered on constructing a mass balance and performance monitoring system for an integrated oil refinery. The objective is to transform raw transactional records from inbound crude oil deliveries—including specific gravity and chemical composition—and outbound product shipments like gasoline, jet fuel, and raffinates into a reconciled analytical framework. The transformation logic must account for volumetric changes and processing losses to establish an accurate daily material balance across the facility. The final warehouse architecture should feature structured tables that aggregate yields and conversion rates by specific process units, enabling engineers to evaluate the efficiency of unit operations and identify performance deviations relative to theoretical benchmarks.


### API 584 IOW Data Warehouse
Design a data engineering project to implement a Risk-Based Integrity Operating Window (IOW) monitoring system compliant with API 584 for a refinery’s static equipment. The scope must include the ingestion and harmonization of high-frequency sensor telemetry (Pressure, Temperature, pH, and flow) with a centralized Asset Registry containing hierarchical IOW limits (Critical, Standard, and Informational). Transformations should identify excursions through time-series analysis, calculating core metrics for frequency, duration, and severity—specifically utilizing "Area Under the Curve" logic to quantify cumulative damage. The resulting data marts must prioritize "Bad Actor" assets, calculate a rolling "Integrity Health Index," and generate automated alerts for inspection schedule adjustments where cumulative process stress indicates accelerated metallurgical degradation or encroachment on design life limits.