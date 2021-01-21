from experimental_notebook.stac_catalog_builder import StacCatalogBuilder
from experimental_notebook.tds_crawler import TDSCrawler


def main():
    tds_crawler = TDSCrawler()
    tds_crawler.run()

    stacCatalogBuilder = StacCatalogBuilder()
    stacCatalogBuilder.build()


if __name__ == "__main__":
    main()