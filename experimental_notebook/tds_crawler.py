from siphon.catalog import TDSCatalog


class TDSCrawler(object):
    def run(self):
        """
        Crawl TDS.
        """
        top_cat = TDSCatalog(
            "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/catalog.xml")
        tds_ds = self.parse_datasets(top_cat)

        print("[INFO] Finished crawling TDS")

        return tds_ds

    def parse_datasets(self, catalog):
        """
        Collect all available datasets.
        """
        datasets = []

        for dataset_name, dataset_obj in catalog.datasets.items():
            http_url = dataset_obj.access_urls.get("httpserver", "")
            odap_url = dataset_obj.access_urls.get("opendap", "")
            ncml_url = dataset_obj.access_urls.get("ncml", "")
            uddc_url = dataset_obj.access_urls.get("uddc", "")
            iso_url = dataset_obj.access_urls.get("iso", "")
            wcs_url = dataset_obj.access_urls.get("wcs", "")
            wms_url = dataset_obj.access_urls.get("wms", "")

            item = {
                "dataset_name" : dataset_name,
                "http_url" : http_url,
                "odap_url" : odap_url,
                "ncml_url" : ncml_url,
                "uddc_url" : uddc_url,
                "iso_url" : iso_url,
                "wcs_url" : wcs_url,
                "wms_url" : wms_url
            }

            item = self.add_tds_ds_metadata(item)
            datasets.append(item)

            # TODO remove
            break

        for catalog_name, catalog_obj in catalog.catalog_refs.items():
            d = self.parse_datasets(catalog_obj.follow())
            datasets.extend(d)

            # TODO : remove, only to have a small dataset
            break

        return datasets


    def add_tds_ds_metadata(self, ds):
        """
        Add extra metadata to item.
        """
        # TODO : replace with regexes
        # TODO : Extract metadata from ncml_url and iso_url
        extra_meta = {
            "model" : "BCCAQv2+ANUSPLIN300",
            "experiment" : "ensemble-percentiles",
            "frequency" : "YS",
            "modeling_realm" : "historical+allrcps",
            "mip_table" : "",
            "ensemble_member" : "",
            "version_number" : "",
            "variable_name" : "tx_mean",
            "temporal_subset" : "1950-2100"
        }

        return dict(ds, **extra_meta)
