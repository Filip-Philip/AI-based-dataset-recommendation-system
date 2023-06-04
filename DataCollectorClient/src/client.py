import logging as log
from services.ParserBase import ParserBase
log.info("Imported base")
from services.zenodo_parser import ZenodoParser
from services.dataverse_parser import DataverseParser

from pycallgraph import PyCallGraph
log.info('Starting the call graph')
from pycallgraph.output import GraphvizOutput

def your_code_function():
    # Your code goes here
    ZenodoParser()
    DataverseParser()

# Create a PyCallGraph object
graph = PyCallGraph(output=GraphvizOutput(output_file='code_structure_graph.png'))

# Start recording the code execution
with graph:
    # Call your function or code block that you want to visualize
    your_code_function()

exit(0)
parsers = []
parsers.append(ZenodoParser())
parsers.append(DataverseParser())



# Create a PyCallGraph object
graph = PyCallGraph(output=GraphvizOutput(output_file='code_structure_graph.png'))

# Start recording the code execution
with graph:
    # Call your function or code block that you want to visualize
    your_code_function()

exit(0)
parsers = []
parsers.append(ZenodoParser())
parsers.append(DataverseParser())


