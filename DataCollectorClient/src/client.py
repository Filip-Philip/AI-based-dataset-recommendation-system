from services.pennsieve_parser import PennsieveParser
from services.dryad_parser import DryadParser
from services.zenodo_parser import ZenodoParser
from services.ParserBase import ParserBase


parsers = []
parsers.append(PennsieveParser())
parsers.append(ZenodoParser())
parsers.append(DryadParser())


