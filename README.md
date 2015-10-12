# sor_ipython
To run against dse:

bin/dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 <full path to aggs.py>

Make sure you run in a venv with pip dependencies installed.

To install pip dependencies:

http://docs.python-guide.org/en/latest/dev/virtualenvs/

and then run the pip installs in the pip_dependencies.txt file.
