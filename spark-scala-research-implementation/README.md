# BDA Assignment 4 — Spark & Scala Research Implementation

Implementation of a Big Data Analytics research paper using Apache Spark and Scala. Replicates the paper's methodology in PySpark, with comparative analysis and academic deliverables.

## Use Case

Understanding how to translate a published research paper's distributed data processing methodology into runnable PySpark code — a common skill in academic and industry research engineering roles.

## Features

- PySpark implementation of the research paper's core algorithm
- Original Scala source code from the paper included for reference
- Comparative analysis between the paper's approach and the implementation
- Jupyter Notebook format for step-by-step walkthrough

## Tech Stack

| Layer | Tools |
|---|---|
| Language | Python 3.8+, Scala |
| Processing | Apache Spark 3.x, PySpark |
| Notebook | Jupyter Notebook |
| Runtime | Java 11 or 17 |

## Prerequisites

- Python 3.8 or higher
- **Java 11 or 17** — verify with `java -version`
- Apache Spark 3.x — verify with `spark-submit --version`
- Jupyter Notebook

## Installation

```bash
pip install pyspark jupyter
```

> If Spark is not installed system-wide, `pyspark` installed via pip includes a bundled Spark runtime.

## Running

**PySpark notebook:**
```bash
jupyter notebook implementation.ipynb
```

**Scala source (requires Spark installed separately):**
```bash
spark-submit originalSourceCode.scala
```

## Project Structure

```
├── implementation.ipynb           # PySpark implementation notebook (run this)
├── originalSourceCode.scala       # Scala source code from the research paper
├── information-13-00058-v2.pdf    # Original research paper (reference)
├── researchpaper.docx             # Written analysis report
├── final.pdf                      # Final report / output
└── slides.pdf                     # Presentation slides
```

## Output & Results

The notebook produces distributed computation results matching the paper's expected output. The final report (`final.pdf`, `researchpaper.docx`) contains the comparative analysis between the paper's approach and the PySpark implementation.

## Notes

- The `originalSourceCode.scala` file is reference material from the paper — it is not a standalone runnable script without the paper's specific dataset
- The PySpark notebook (`implementation.ipynb`) is the primary deliverable and can run with the bundled Spark from the `pyspark` pip package
