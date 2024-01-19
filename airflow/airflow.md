```mermaid

flowchart TB

    subgraph TwitterDataCollection
        A[Data Collection]
    end

    subgraph Data Cleaning
        B[Execute PySpark Notebook]
    end

    subgraph ML Pipeline
        C[Run MLflow Pipeline]
    end

    subgraph Inferring Task
        D[Run Transformers Model]
    end

    A --> B
    B --> C
    C --> D
```