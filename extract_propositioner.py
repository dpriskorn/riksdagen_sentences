from models.riksdagen_analyzer import RiksdagenAnalyzer


ra = RiksdagenAnalyzer(workdirectory="data/sv/propositioner", filename="propositioner")
ra.start()
# print(ra.df)
