from models.riksdagen_analyzer import RiksdagenAnalyzer


ra = RiksdagenAnalyzer(
    workdirectory="data/sv/departementserien", filename="departementserien_test"
)
ra.start()
# print(ra.df)
