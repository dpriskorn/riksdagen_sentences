from models.riksdagen_analyzer import RiksdagenAnalyzer

if __name__ == "__main__":
    analyzer = RiksdagenAnalyzer()
    analyzer.load_jsonl()
    analyzer.print_statistics()
