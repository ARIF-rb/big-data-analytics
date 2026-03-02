# BDA Assignment 4 — Spark & Scala Research Implementation

Implementation of a research paper on Big Data Analytics using Apache Spark and Scala. Includes analysis notebook, original source code, and academic deliverables.

## Tech Stack

- **Apache Spark / PySpark** — distributed data processing
- **Scala** — original source code from research paper
- **Jupyter Notebook** — Python implementation

## Project Structure

```
├── implementation.ipynb          # PySpark implementation notebook
├── originalSourceCode.scala      # Scala source code from the paper
├── information-13-00058-v2.pdf   # Research paper (reference)
├── final.pdf                     # Final report / output
├── researchpaper.docx            # Research paper write-up
└── slides.pdf                    # Presentation slides
```

## Prerequisites

- Python 3.8+
- Java 11 or 17
- Apache Spark 3.x
- Jupyter Notebook

## Installation & Running

```bash
pip install pyspark jupyter
jupyter notebook implementation.ipynb
```

To compile and run the Scala source directly:

```bash
spark-submit originalSourceCode.scala
```

## Overview

This assignment implements and analyzes a research paper referenced as `information-13-00058-v2`, replicating its methodology using PySpark. The implementation covers distributed data processing concepts explored in the paper with a comparative analysis in the report.
