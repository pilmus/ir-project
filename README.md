# IR Project Experiments

This repository contains all code necessary to reproduce the experiments in the project.

## Prerequisites

- A fully indexed MS MARCO
  dataset. [Follow this guide](https://github.com/castorini/anserini/blob/master/docs/experiments-msmarco-doc.md).
- A fully indexed MS MARCO
  dataset, augmented with predicted queries. [Follow this guide](https://github.com/castorini/docTTTTTquery#Replicating-MS-MARCO-Document-Ranking-Results-with-Anserini).
- Java 11+, Maven

## Performing experiments

At the moment we do not provide easy command-line access, since a lot of experiments require small modifications to the
source code to change the setup. It is recommended to open this project in a Java IDE.

Some general pointers:

- `nl.tudelft.ir.LibSvmFileGenerator` is a runnable class that is able to create a training dataset for training
  LambdaMART.

- `nl.tudelft.ir.BM25LRunGenerator` is a runnable class that is used to do a retrieval run with the BM25L function.

- `nl.tudelft.ir.RerankRunGenerator` is a runnable class that is used to do a retrieval run with the BM25 + LambdaMART
  setup.

- `nl.tudelft.ir.features` contains the classes that are used for LambdaMART feature generation.
- `nl.tudelft.ir.features.Features` this class contains a list of all the features used for LambdaMART.

