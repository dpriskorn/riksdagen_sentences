from models.riksdagen_analyzer import RiksdagenAnalyzer


ra = RiksdagenAnalyzer(
    riksdagen_document_type="departementserien"
)
ra.start()
# print(ra.df)
