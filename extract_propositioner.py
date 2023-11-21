from models.riksdagen_analyzer import RiksdagenAnalyzer


ra = RiksdagenAnalyzer(
    riksdagen_document_type="proposition"
)
ra.start()
# print(ra.df)
