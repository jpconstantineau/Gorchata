# Oil Refinery Data Warehousing - Documentation Index

## Overview

This directory contains comprehensive technical documentation for the Oil Refinery Data Transformation and Warehousing project. The documentation covers refinery operations, measurement standards, catalyst management, mass balance principles, and other essential topics for understanding petroleum refining analytics.

## Documentation Files

### [MEASUREMENT_STANDARDS.md](MEASUREMENT_STANDARDS.md)
**Comprehensive guide to petroleum measurement standards**

Topics covered:
- API gravity calculations and classifications
- Volume to mass conversions
- Temperature corrections and Volume Correction Factors (VCF)
- Quality specifications (sulfur, RVP, octane, cetane, distillation)
- Mass balance accounting standards
- Measurement uncertainty and tolerances

**Use this document when:**
- Converting between volume (barrels) and mass (tons)
- Applying temperature corrections to tank measurements
- Understanding crude oil and product property classifications
- Implementing quality specification validations
- Troubleshooting measurement discrepancies

### [CATALYST_MANAGEMENT.md](CATALYST_MANAGEMENT.md)
**Detailed guide to refinery catalyst lifecycle and management**

Topics covered:
- Catalyst fundamentals (surface area, pore structure, active sites)
- Major refinery catalysts (FCC, Reformer, Hydrocracker, Hydrotreater)
- Catalyst lifecycle stages (Fresh, Early-Mid, Mid-Cycle, Late-Mid, End-of-Run)
- Deactivation mechanisms and performance degradation
- Regeneration processes and economics
- Optimization and decision frameworks

**Use this document when:**
- Designing the dim_catalyst_cycle dimension
- Understanding catalyst performance correlations
- Implementing predictive maintenance models
- Analyzing catalyst economics and lifecycle costs
- Troubleshooting unit performance issues

### [MASS_BALANCE.md](MASS_BALANCE.md)
**Mass balance principles and reconciliation methods**

Topics covered:
- Conservation of mass fundamentals
- Volume vs. mass balance concepts
- Refinery-wide mass balance (inputs, outputs, inventory)
- Unit-level mass balances (CDU, FCC, Hydrocracker, Reformer)
- Reconciliation methods (daily, weekly, monthly)
- Variance troubleshooting and investigation procedures
- Advanced topics (statistical process control, linear programming, sustainability)

**Use this document when:**
- Designing fact tables for production tracking
- Implementing mass balance validation tests
- Understanding refinery yields and process economics
- Troubleshooting inventory discrepancies
- Building reconciliation reports and dashboards

## Document Relationships

### For Schema Design (Phase 1)
1. Start with [MEASUREMENT_STANDARDS.md](MEASUREMENT_STANDARDS.md) to understand:
   - Property fields needed (API gravity, sulfur, specific gravity, etc.)
   - Measurement units and ranges for validation
   - Quality specification attributes

2. Review [CATALYST_MANAGEMENT.md](CATALYST_MANAGEMENT.md) for:
   - dim_catalyst_cycle structure and fields
   - Performance metrics to track
   - Catalyst age and efficiency relationships

3. Consult [MASS_BALANCE.md](MASS_BALANCE.md) for:
   - Input/output stream types
   - Inventory management requirements
   - Unit-level production tracking needs

### For Fact Table Design (Phase 2-3)
1. [MASS_BALANCE.md](MASS_BALANCE.md) provides:
   - Fact grain definition (daily, by unit, by stream)
   - Measures to track (volume, mass, temperature, quality properties)
   - Inventory change calculations

2. [MEASUREMENT_STANDARDS.md](MEASUREMENT_STANDARDS.md) informs:
   - Conversion logic (volume to mass)
   - Temperature correction factors
   - Quality measures and tolerances

3. [CATALYST_MANAGEMENT.md](CATALYST_MANAGEMENT.md) supports:
   - Catalyst tracking facts
   - Performance correlation analyses
   - Regeneration event recording

### For Data Quality Testing (Phase 5)
All three documents provide:
- Acceptable ranges for numeric values
- Referential integrity requirements
- Business rule validations
- Mass balance closure criteria

### For Analytics (Phase 6-7)
- [MASS_BALANCE.md](MASS_BALANCE.md): Yield analysis, optimization models
- [CATALYST_MANAGEMENT.md](CATALYST_MANAGEMENT.md): Predictive maintenance, economic optimization
- [MEASUREMENT_STANDARDS.md](MEASUREMENT_STANDARDS.md): Quality compliance, seasonal spec management

## Additional Documentation (Future)

### Planned Documentation
The following documents will be added in future phases:

1. **PROCESS_UNITS.md**
   - Detailed operating principles for each unit type
   - Typical operating conditions and ranges
   - Equipment configurations and diagrams
   - Performance KPIs by unit type

2. **CRUDE_PROPERTIES.md**
   - Comprehensive crude assay data
   - Crude selection economics
   - Blending strategies
   - Regional crude characteristics

3. **PRODUCT_SPECIFICATIONS.md**
   - Detailed specifications by product grade
   - Regulatory requirements (EPA, ASTM, etc.)
   - Regional specification variations
   - Seasonal specification transitions

4. **DOWNTIME_MANAGEMENT.md**
   - Planned turnaround scheduling
   - Unplanned downtime categorization
   - Reliability metrics and targets
   - Economic impact analysis

5. **ENERGY_MANAGEMENT.md**
   - Refinery energy balance
   - Fuel gas consumption tracking
   - Steam and power generation
   - Energy efficiency metrics

6. **BLENDING_OPTIMIZATION.md**
   - Gasoline blending recipes
   - Diesel blending strategies
   - Property blending calculations
   - Economic optimization models

7. **DATA_DICTIONARY.md**
   - Complete field definitions
   - Business terminology glossary
   - Unit of measure standards
   - Naming conventions

## Document Maintenance

### Ownership
- **Technical Content**: Subject matter experts in refining operations
- **Data Modeling**: Data warehouse team
- **Quality Assurance**: Technical reviewers

### Update Frequency
- **Measurement Standards**: Review annually for regulatory changes
- **Catalyst Management**: Update as new catalyst types introduced
- **Mass Balance**: Update with process changes or new units

### Version Control
All documentation is maintained in version control with:
- Change tracking
- Review/approval workflow
- Issue tracking for corrections/enhancements

## References and Standards

### Industry Organizations
- **API** (American Petroleum Institute): Measurement standards, data books
- **ASTM** (American Society for Testing and Materials): Test methods, specifications
- **EIA** (Energy Information Administration): Reporting standards, definitions

### Technical Publications
- Gary, J.H., Handwerk, G.E., Kaiser, M.J. (2007). *Petroleum Refining: Technology and Economics*
- Speight, J.G. (2014). *The Chemistry and Technology of Petroleum*
- Jones, D.S.J., Pujado, P.R. (2006). *Handbook of Petroleum Processing*

### Regulatory
- **EPA**: Environmental regulations, fuel specifications
- **DOT**: Transportation and safety standards
- **OSHA**: Workplace safety and health standards

## Getting Started

### For Data Modelers
1. Read [MASS_BALANCE.md](MASS_BALANCE.md) Section "Refinery-Wide Mass Balance" to understand data flows
2. Review [MEASUREMENT_STANDARDS.md](MEASUREMENT_STANDARDS.md) Section "API Gravity" and "Volume and Mass Conversions"
3. Study [CATALYST_MANAGEMENT.md](CATALYST_MANAGEMENT.md) Section "Catalyst Lifecycle Stages"

### For Developers
1. Understand conversion formulas in [MEASUREMENT_STANDARDS.md](MEASUREMENT_STANDARDS.md)
2. Study validation rules in all three documents
3. Review unit-level balances in [MASS_BALANCE.md](MASS_BALANCE.md)

### For Business Analysts
1. Start with README.md in parent directory for business context
2. Focus on "Business Context" sections in each document
3. Review economic optimization sections

### For Data Quality Engineers
1. Extract validation rules from each document
2. Define acceptable tolerance ranges
3. Build test cases based on documented examples

## Support

For questions or clarifications on documentation:
- Technical questions: Contact refining operations SMEs
- Data modeling questions: Contact data warehouse team
- Documentation issues: Submit via issue tracker

## License

This documentation is part of the Gorchata project and follows the same license terms.
