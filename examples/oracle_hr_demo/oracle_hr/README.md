# oracle_hr — pipeline project

openmedallion project config for the Oracle HR demo.

```
oracle_hr/
├── main.yaml          # pipeline name + layer wiring
├── backend/
│   ├── bronze.yaml    # dlt sql_database source (Oracle)
│   ├── silver.yaml    # type casts for Oracle column types
│   └── gold.yaml      # aggregations: dept_summary, job_summary, employee_mobility
├── ipynb/
│   └── walkthrough.ipynb
└── frontend/
    ├── powerbi/
    └── tableau/
```

### Gold outputs

| File | Description |
|------|-------------|
| `dept_summary.parquet` | headcount + total_payroll grouped by department_id |
| `job_summary.parquet` | employee_count + total_payroll grouped by job_id |
| `employee_mobility.parquet` | job_changes count per employee_id |

> avg_salary = total_payroll / headcount (compute in notebook or BI tool)
