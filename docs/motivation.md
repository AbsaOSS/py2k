# Why we built Py2K

**Py2K** is a library that connects pandas dataframes to Kafka and Schema Registry. It is a tool **designed by data scientists, for data scientists**. It allows data scientists, data engineers and developers to stay within the Python environment and avoid the jargon related to Kafka, Schema Registry,and Avro by utilising Pythonic Objects.

---

If you look online for examples of how to post data from pandas onto Kafka with integration into the Confluent Schema Registry, you won't find many of them. While well documented for general Kafka Producers, they become increasingly complex to implement, which can easily lead you to write more code for getting your data to Kafka than the actual project use-case.

## Remove Bloat

We wanted to create a tool with **minimal overhead** that didn't bloat data science projects with copious amounts of boilerplate code, effectively taking the process **from 100s of lines of code to just 4 lines!** (8 if we want to be [PEP8](https://www.python.org/dev/peps/pep-0008/) compliant)

## Keep Data Scientists doing Data Science

We wanted to **keep data scientists using tools they're comfortable with**, so we provide an API for **automatic conversion of pandas Dataframes**. This is analogous to Absa's [ABRiS](https://github.com/AbsaOSS/ABRiS) package for Spark Dataframes which helps data engineers by providing similar functionality.

## Minimal Kafka, Avro, Confluent Jargon

We wanted to abstract data scientists from the tedious job of serializing their data into Avro objects and defining Avro schema for each of their products. Utilising [pydantic](https://github.com/samuelcolvin/pydantic) we're able to automate schema validation and generation for data.

## Usable and Maintained Documentation and Examples

Lastly, we wanted **Py2K** to have usable documentation and examples. As we build out our documentation, each bit of python code has unit tests to ensure that the code provided works for the latest version of **Py2K**.
