# Tupik

Tupik is a tool that is alerting about data anomalies, ensures consistency and awareness of suspicious entries.

# Architecture

Tupik acts as a pipeline of 3 major stages. 

## Stage 1

- Gather stats of data, makes numerous statistical checks for different data divergencies
- 59 methods can be seen in **docs/statistical_methods.py**

## Stage 2

- Numerous AI/ML tools that are made to analyze contents of data and metadata and check for values that are not fitting into constraints in its context and context of the world we live in
- 10 methods can be seen in **docs/aiml_methods.md**

## Stage 3

- Mad (data) scientist tier feature
- Have a help of agentic AI to find inconsistencies in data and find root causes of data anomalies




