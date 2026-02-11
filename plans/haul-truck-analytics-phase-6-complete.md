## Phase 6 Complete: Analytical Queries & Views

Successfully implemented business-focused analytical queries answering key operational questions about bottleneck identification, underperforming trucks, payload compliance, shift productivity, fuel efficiency, and operator performance with actionable insights and recommendations.

**Files created/changed:**
- examples/haul_truck_analytics/models/analytics/worst_performing_trucks.sql
- examples/haul_truck_analytics/models/analytics/bottleneck_analysis.sql
- examples/haul_truck_analytics/models/analytics/payload_compliance.sql
- examples/haul_truck_analytics/models/analytics/shift_performance.sql
- examples/haul_truck_analytics/models/analytics/fuel_efficiency.sql
- examples/haul_truck_analytics/models/analytics/operator_performance.sql
- test/haul_truck_queries_test.go

**Functions created/changed:**
- TestHaulTruckAnalyticsQueriesExist
- TestWorstPerformingTrucksQuery
- TestHaulTruckBottleneckAnalysisQuery
- TestPayloadComplianceQuery
- TestShiftPerformanceQuery
- TestFuelEfficiencyQuery
- TestOperatorPerformanceQuery
- TestQueryResultStructure

**Tests created/changed:**
- TestHaulTruckAnalyticsQueriesExist - validates all 6 analytical SQL files exist
- TestWorstPerformingTrucksQuery - validates ranking by lowest tons per hour with issue identification and performance scoring
- TestHaulTruckBottleneckAnalysisQuery - validates crusher vs shovel constraint identification with bottleneck scoring and recommendations
- TestPayloadComplianceQuery - validates underload (<85%), optimal (95-105%), overload (>105%) tracking with compliance scoring
- TestShiftPerformanceQuery - validates day vs night productivity comparison with variance analysis and recommendations
- TestFuelEfficiencyQuery - validates liters per ton, liters per ton-mile calculations with cost impact analysis
- TestOperatorPerformanceQuery - validates operator ranking by efficiency score with experience-level peer comparison
- TestQueryResultStructure - validates SQL structure for all 6 queries (CTE usage, table references, keywords)

**Key Query Features:**

**worst_performing_trucks.sql:**
- Ranks trucks by tons_per_hour (lowest first) within fleet class
- Calculates weighted performance score (0-100): tons/hour 50%, utilization 30%, cycle time 20%
- Identifies 8 specific issues: low utilization, high cycle times, excessive delays, poor efficiency
- Compares each truck to fleet class averages
- Provides actionable recommendations for improvement

**bottleneck_analysis.sql:**
- Analyzes both crusher and shovel queue patterns
- Calculates bottleneck score (0-100): queue time 40%, total hours 30%, utilization 30%
- Categorizes: CRITICAL BOTTLENECK / BOTTLENECK / CONSTRAINT / NORMAL
- Ranks constraints by investment priority
- Provides capacity expansion recommendations

**payload_compliance.sql:**
- Classifies cycles: UNDERLOAD (<85%), ACCEPTABLE (85-95%), OPTIMAL (95-105%), OVERLOAD (>105%)
- Calculates compliance score: penalizes overloads 2x for safety emphasis
- Tracks violation patterns by truck and operator
- Considers operator experience in recommendations
- Identifies 8 trend categories from "SAFETY RISK" to "EXCELLENT"

**shift_performance.sql:**
- Compares day vs night shifts on productivity metrics
- Calculates tons per hour, cycles per truck per day, tons per truck per day
- Compares shifts to system average and best shift
- Embeds actual metric values in variance explanations
- Provides 5 recommendation levels from CRITICAL to EXCELLENCE

**fuel_efficiency.sql:**
- Calculates liters per ton (primary efficiency metric)
- Adds distance-adjusted metric: liters per ton-mile
- Ranks trucks within fleet class by efficiency
- Estimates fuel costs ($1.50/liter) and savings potential vs fleet average
- Provides 5 efficiency assessments with maintenance recommendations

**operator_performance.sql:**
- Calculates efficiency score (0-100): productivity 40%, utilization 30%, cycle time 20%, delays 10%
- Dual ranking: overall performance AND within experience level
- Compares operators to experience-level peers
- Provides experience-appropriate recommendations (8 categories)
- Recognizes top performers and identifies training needs

**SQL Quality Features:**
- Consistent CTE architecture: data → calculation → ranking → assessment
- Defensive programming with NULLIF() for division-by-zero protection
- Proper window functions (ROW_NUMBER, RANK with PARTITION BY)
- Comparative analytics benchmarking against meaningful comparisons
- Meaningful derived columns with clear naming
- Appropriate numeric rounding for display
- Business documentation at query header
- Gorchata integration with {{ ref }} and {{ config }} syntax

**Review Status:** APPROVED

**Business Value:**
- All queries provide actionable insights, not just raw metrics
- Direct cost impact analysis (fuel efficiency)
- Safety compliance tracking (payload overloads)
- Investment prioritization (bottleneck analysis)
- Training needs identification (operator performance)
- Root cause analysis for underperformance

**Git Commit Message:**
```
feat: Add analytical queries and views (Phase 6/8)

- Implement worst performing trucks query with performance scoring and issue identification
- Add bottleneck analysis comparing crusher vs shovel constraints with prioritized recommendations
- Create payload compliance tracking with safety-weighted scoring across 4 utilization bands
- Build shift performance comparison with variance analysis and operational recommendations
- Generate fuel efficiency analysis with cost impact and distance-adjusted metrics
- Develop operator performance ranking with experience-level peer comparisons
- Add 8 passing tests validating query structure and business logic
```
