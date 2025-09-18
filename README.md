# Electify Youth - Data Analysis & Business Intelligence

[![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Tableau](https://img.shields.io/badge/Tableau-Desktop-orange?logo=tableau&logoColor=white)](https://tableau.com)
[![Google Cloud](https://img.shields.io/badge/Google_Cloud-Platform-blue?logo=google-cloud&logoColor=white)](https://cloud.google.com)
[![Pandas](https://img.shields.io/badge/Pandas-Data_Analysis-green?logo=pandas&logoColor=white)](https://pandas.pydata.org)
[![Docker](https://img.shields.io/badge/Docker-Containerization-blue?logo=docker&logoColor=white)](https://docker.com)



## Overview

This repository contains the **Data Analysis and Business Intelligence** component of the **Electify Youth** integration project. While the main application focuses on engaging youth through surveys and questionnaires, this data warehouse solution provides comprehensive analytics and insights into youth engagement patterns.

### Key Features:
- **📊 Dimensional Data Warehouse**: Star schema design optimized for analytical queries
- **🔄 Multi-Environment ETL Pipeline**: Local, test, and production environments
- **📈 Youth Engagement Analytics**: Deep insights into survey participation patterns
- **🌍 Geographic Analysis**: Country-wise engagement tracking
- **📅 Temporal Analysis**: Seasonal and time-based trend analysis
- **🎯 Theme & Sub-theme Analytics**: Understanding youth interests and preferences
- **📊 Interactive Dashboards**: Tableau visualizations for business intelligence

---

## Project Architecture

### Data Warehouse Structure (Star Schema)

```
Fact_Survey (Central Fact Table)
├── dim_Date (Time Dimension)
├── dim_Country (Geographic Dimension)
├── dim_Organization (Organization Dimension)
├── dim_Theme (Survey Theme Dimension)
├── dim_SubTheme (Sub-theme Dimension)
├── dim_Question_Type (Question Classification)
├── dim_Question (Individual Questions)
├── Survey_Question_Bridge (Many-to-Many)
└── Survey_Country_Bridge (Many-to-Many)
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Database** | PostgreSQL | Data storage and analytics |
| **ETL** | Python + psycopg2 | Data extraction and transformation |
| **Processing** | Pandas | Data manipulation and analysis |
| **Visualization** | Tableau | Business intelligence dashboards |
| **Cloud** | Google Cloud Platform | Production deployment |
| **Orchestration** | Python Scripts | ETL pipeline management |

---

## Business Intelligence Capabilities

### Key Analytics Questions Answered:
1. **🎯 Theme Popularity**: Which themes are most interesting to youth?
2. **📈 Engagement Trends**: What trends can be observed in youth engagement over time?
3. **❓ Question Type Impact**: How do different question types affect youth engagement?
4. **🗓️ Seasonal Patterns**: Which themes are popular during specific seasons?
5. **🏢 Organization Performance**: Which organizations get the most questions?
6. **🔍 Sub-theme Analysis**: What subthemes interest youth the most?
7. **🌍 Geographic Insights**: Which countries have the most youth engagement?
8. **📊 Trending Topics**: What are the trending subthemes this year?

---

## Project Structure

```
data_analysis/
├── 📁 data_warehouse/              # Core data warehouse implementation
│   ├── 📁 database_connection/     # Database configuration management
│   ├── 📁 dimension/              # Local development environment
│   ├── 📁 testData_Dimension/     # Test environment
│   └── 📁 prodDWH_Dimension/      # Production environment
├── 📁 visualization/              # Tableau dashboards and reports
└── 📁 docs/                     
```

---

## Getting Started

### Prerequisites
- Python 3.x
- PostgreSQL 13+
- Tableau Desktop (for visualization)
- Docker (for database setup) 

### Database Setup

1. **Start the databases using Docker:**
   ```bash
   docker-compose up
   ```
   
   **Note:** Docker is used in the main Electify Youth Java application (the integration project) to spin up all databases, not in this data analysis component. This data analysis project connects to the databases spun up by the main application.

2. **Configure database connections:**
   - Update connection settings in `data_warehouse/database_connection/`
   - Ensure proper credentials for local, test, and production environments
   
### Running the ETL Pipeline

1. **Navigate to the data warehouse directory:**
   ```bash
   cd data_warehouse/dimension
   ```

2. **Execute the main ETL pipeline:**
   ```bash
   python main.py
   ```

   This will:
   - Create all dimensional tables
   - Populate dimension tables with data
   - Load fact tables
   - Establish relationships through bridge tables

### Environment-Specific Execution

- **Local Development**: `cd data_warehouse/dimension && python main.py`
- **Test Environment**: `cd data_warehouse/testData_Dimension && python main.py`
- **Production**: `cd data_warehouse/prodDWH_Dimension && python main.py`

---

## Data Sources

### Operational Database (ElectifyDB)
- **Organizations**: Institution and organization data
- **Questionnaires**: Survey definitions and metadata
- **Questions**: Individual survey questions
- **Answers**: Youth responses and engagement data
- **Themes/Subthemes**: Categorization of survey content

### Data Warehouse (ElectifyDB_dwh)
- **Dimensional Model**: Optimized for analytical queries
- **Historical Data**: Time-series analysis capabilities
- **Aggregated Metrics**: Pre-calculated KPIs and measures

---

## Analytics & Visualization

### SQL Analytics
Pre-built analytical queries are available in `analytics.sql` covering:
- Theme popularity analysis
- Temporal engagement trends
- Question type distribution
- Seasonal pattern analysis
- Geographic engagement metrics

### Tableau Dashboards
- **Interactive Visualizations**: Dynamic charts and graphs
- **Real-time Data**: Live connection to data warehouse
- **Multi-dimensional Analysis**: Drill-down capabilities
- **Export Functionality**: PDF and image export options

---

## Configuration

### Database Connections
The project supports multiple database environments:

| Environment | Configuration File | Purpose |
|-------------|-------------------|---------|
| Local | `database.ini` | Development |
| Test | `database_test.ini` | Testing |
| Production | `databaseDWHgcloud.ini` | Production |
| Source DB | `databaseProdDB.ini` | Source system |

### Environment Variables
Ensure proper database credentials are configured in the respective `.ini` files.

---

## Integration with Main Project

This data analysis component integrates with the main **Electify Youth** application:

- **Data Source**: Consumes survey data from the main application's PostgreSQL database
- **Real-time Updates**: ETL pipeline can be scheduled for regular data updates
- **Business Intelligence**: Provides insights back to administrators and super administrators
- **Reporting**: Enables data-driven decision making for survey content and timing

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly across all environments
5. Submit a pull request

---

## License

This project is part of the KDG-TI Integration 4 course (2023-2024) - Team 8.
