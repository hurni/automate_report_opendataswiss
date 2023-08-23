# -*- coding: utf-8 -*-
"""
Created on Wed May 24 10:34:45 2023
@author: U80838962
"""


# required modules
import math
import pandas as pd
import requests as r
import time
from datetime import datetime
import urllib3
from sys import exit
from os import remove


"""
## define variables:
## bund : Are you working within the bundesnetz resp. via vpn?
## test : Do you want to test the script or get all the information declared 
##        as wanted_package_info, wanted_organization_info and 
##        wanted_resource_info.
"""


BUND = False
TEST = False


def is_bv_netz(BUND):
    if BUND:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        proxies = {
            "http": "http://proxy-bvcol.admin.ch:8080",
            "https": "http://proxy-bvcol.admin.ch:8080",
        }
    else:
        proxies = None
    return proxies


def test_completeness_csv(proxies):
    s = r.Session()
    """ get list of orgnizations """
    listed_organizations = s.get(
        "https://ckan.opendata.swiss/api/3/action/organization_list",
        verify=False,
        proxies=proxies,
    ).json()["result"]
    organizations = []
    for slug in listed_organizations:
        try:
            s.get(
                f"https://ckan.opendata.swiss/api/3/action/package_search?fq=organization:{slug}",
                verify=False,
                proxies=proxies,
            ).json()["result"]["results"][0]["organization"]["political_level"]
            organizations.append(slug)
        except IndexError:
            """
            # some organizations do not have packages as only their
            # underorganisations publish. Skip these organizations
            """
            continue

    df_slug_toplevel_names = pd.read_csv("dict_slug_fr_it_de_en.csv")

    for slug in organizations:
        if slug not in df_slug_toplevel_names["organisation_slug"].unique():
            exit(
                f"Organisation is not up to date. Add {slug} to \
dict_slug_fr_it_de_en.csv"
            )

    return organizations, df_slug_toplevel_names, listed_organizations


def slug_packages(organizations, proxies):
    """
    # create dictionary with organization-slugs and corresponding number
    # of packages
    """
    s = r.Session()
    slugs_packages_dict = {}
    for slug in organizations:
        nr_packages = s.get(
            f"https://ckan.opendata.swiss/api/3/action/package_search?fq=organization:{slug}",
            verify=False,
            proxies=proxies,
        ).json()["result"]["count"]
        slugs_packages_dict[slug] = nr_packages
    nr_packages = []
    for slug in slugs_packages_dict:
        nr_packages.append(slugs_packages_dict[slug])
    return slugs_packages_dict, nr_packages


def slug_names(slugs_packages_dict, df_slug_toplevel_names, organizations):
    fr_slug_name = pd.Series(
        df_slug_toplevel_names.top_level_name_fr.values,
        index=df_slug_toplevel_names.organisation_slug,
    ).to_dict()
    it_slug_name = pd.Series(
        df_slug_toplevel_names.top_level_name_it.values,
        index=df_slug_toplevel_names.organisation_slug,
    ).to_dict()
    de_slug_name = pd.Series(
        df_slug_toplevel_names.top_level_name_de.values,
        index=df_slug_toplevel_names.organisation_slug,
    ).to_dict()
    en_slug_name = pd.Series(
        df_slug_toplevel_names.top_level_name_en.values,
        index=df_slug_toplevel_names.organisation_slug,
    ).to_dict()
    names_fr = []
    names_it = []
    names_de = []
    names_en = []
    for slug in organizations:
        names_fr.append(fr_slug_name[slug])
    for slug in organizations:
        names_it.append(it_slug_name[slug])
    for slug in organizations:
        names_de.append(de_slug_name[slug])
    for slug in organizations:
        names_en.append(en_slug_name[slug])

    return names_fr, names_it, names_de, names_en


def slug_federal_level(organizations, df_slug_toplevel_names, proxies):
    """
    ## create dictionary with slugs and federal level using pandas.DataFrame()
    """
    s = r.Session()
    levels_en = []
    for i in organizations:
        political_level = s.get(
            f"https://ckan.opendata.swiss/api/3/action/package_search?fq=organization:{i}",
            verify=False,
            proxies=proxies,
        ).json()["result"]["results"][0]["organization"]["political_level"]
        levels_en.append(political_level)
    federal_level_dict = pd.read_csv("dict_federallevels_fr_it_de_en.csv")
    fr_federal_level_dict = pd.Series(
        federal_level_dict.fr.values, index=federal_level_dict.en
    ).to_dict()
    it_federal_level_dict = pd.Series(
        federal_level_dict.it.values, index=federal_level_dict.en
    ).to_dict()
    de_federal_level_dict = pd.Series(
        federal_level_dict.de.values, index=federal_level_dict.en
    ).to_dict()
    levels_fr = []
    levels_it = []
    levels_de = []
    for slug in levels_en:
        levels_fr.append(fr_federal_level_dict[slug])
    for slug in levels_en:
        levels_it.append(it_federal_level_dict[slug])
    for slug in levels_en:
        levels_de.append(de_federal_level_dict[slug])

    return levels_fr, levels_it, levels_de, levels_en


def create_dataframe(
    organizations,
    nr_packages,
    names_fr,
    names_it,
    names_de,
    names_en,
    levels_fr,
    levels_it,
    levels_de,
    levels_en,
):
    df = pd.DataFrame(
        columns=[
            "slugs",
            "nr_packages",
            "political_level",
            "toplevel_fr",
            "toplevel_it",
            "toplevel_de",
            "toplevel_en",
            "levels_fr",
            "levels_it",
            "levels_de",
            "levels_en",
        ]
    )
    df["slugs"] = organizations
    df["nr_packages"] = nr_packages
    df["toplevel_fr"] = levels_en
    df["toplevel_fr"] = names_fr
    df["toplevel_it"] = names_it
    df["toplevel_de"] = names_de
    df["toplevel_en"] = names_en
    df["levels_en"] = levels_en
    df["levels_de"] = levels_de
    df["levels_fr"] = levels_fr
    df["levels_it"] = levels_it
    dataframe = df

    return dataframe


def create_csv_from_dataframe(dataframe):
    toplevels = ["toplevel_de", "toplevel_fr", "toplevel_it", "toplevel_en"]
    for index, value in enumerate(toplevels):
        df_top = dataframe[[value, "nr_packages"]].copy()
        df_top["nr_packages"] = df_top.groupby([value])["nr_packages"].transform("sum")
        df_top = df_top.drop_duplicates()
        df_top_report = df_top.sort_values("nr_packages", ascending=False).copy()

        """ #dep_level """
        df_dep = dataframe[["nr_packages", "levels_en", value]].copy()
        df_dep = df_dep.loc[df_dep["levels_en"] == "confederation"]
        df_dep["nr_packages"] = df_dep.groupby([value])["nr_packages"].transform("sum")
        df_dep = df_dep[[value, "nr_packages"]]
        df_dep = df_dep.drop_duplicates()
        df_dep_report = df_dep.sort_values("nr_packages", ascending=False).copy()
    """ # fed_pack"""
    levels = ["levels_de", "levels_fr", "levels_it", "levels_en"]
    for index, value in enumerate(levels):
        df_fed = dataframe[[value, "nr_packages"]].copy()
        df_fed["nr_packages"] = df_fed.groupby([value])["nr_packages"].transform("sum")
        df_fed = df_fed.drop_duplicates()
        df_fed_report = df_fed.sort_values("nr_packages", ascending=False).copy()

    return df_top_report, df_dep_report, df_fed_report


def fetch_packages(TEST, proxies):
    results = []
    s = r.Session()
    total_packages = s.get(
        "https://ckan.opendata.swiss/api/3/action/package_search",
        proxies=proxies,
        verify=False,
    ).json()["result"]["count"]
    if TEST:
        limit_set = 100
        nr_runs = 3
    else:
        limit_set = 1000
        nr_runs = int(math.ceil(total_packages / limit_set))
    for i in range(nr_runs):
        query_list = s.get(
            f"http://opendata.swiss/api/3/action/current_package_list_with_resources?offset={i*limit_set}&limit={limit_set}",
            proxies=proxies,
            verify=False,
        ).json()["result"]
        results.extend(query_list)

    return results


def from_geocat(results):
    term = "geocat.ch permalink"
    geocat = []
    try:
        for dataset in results:
            relations = dataset[["relations"][0]]
            name = dataset["title_for_slug"]
            # print(value)
            for iter in range(len(relations)):
                if term in relations[iter].values():
                    geocat.append(name)
                else:
                    continue

    except KeyError:
        """some datasets do not use the relations variable"""
        pass

    return geocat


def get_max_resources(results):
    max_resources = 0
    total_resources = 0
    for nr, item in enumerate(results):
        x = results[nr]["num_resources"]
        total_resources = total_resources + x
        if x > max_resources:
            max_resources = x
        else:
            continue

    return max_resources, total_resources


def create_report(
    organizations,
    df_top_report,
    df_dep_report,
    df_fed_report,
    listed_organizations,
    nr_packages,
    max_resources,
    total_resources,
    geocat,
):
    len_dep = len(df_dep_report)
    df_top_report = df_top_report.head(10).style.set_properties(
        **{"background-color": "black", "color": "white", "border-color": "white"}
    )
    df_fed_report = df_fed_report.style.set_properties(
        **{"background-color": "black", "color": "white", "border-color": "white"}
    )
    df_dep_report = df_dep_report.style.set_properties(
        **{"background-color": "black", "color": "white", "border-color": "white"}
    )
    df_top_report_html = df_top_report.to_html(header=False, index=False)
    df_fed_report_html = df_fed_report.to_html(header=False, index=False)
    df_dep_report_html = df_dep_report.to_html(header=False, index=False)

    now = datetime.now()
    now = now.strftime("%d-%m-%Y %H:%M:%S")

    text = f'<p style="background-color:black;color:white;">As of {now}, on opendata.swiss, a total of {len(listed_organizations)} organizations \
are listed. However, {len(organizations)} organizations are actually publishing data.<br>\
In total, there are {sum(nr_packages)} datasets with {total_resources} ressources on \
opendata.swiss. {len(geocat)} are originating from geocat. The maximum number of ressources of any dataset is {max_resources}.<br><br>\
The ten most active organizations are:<br><br> \
{df_top_report_html}<br><br>Considering federal \
levels, the distribution of datasets is as follows:<br><br>\
{df_fed_report_html}<br><br>In total, {len_dep} organizations of the federal level are \
publishing their data on opendata.swiss. The organizations and the corresponding datasets are as follows:<br><br>\
{df_dep_report_html}<br><br></p>'

    with open("report_header.html", "w") as file:
        file.write(text)

    return text


def main(BUND, TEST):
    start = time.perf_counter()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    proxies = is_bv_netz(BUND)
    organizations, df_slug_toplevel_names, listed_organizations = test_completeness_csv(
        proxies
    )
    slugs_packages_dict, nr_packages = slug_packages(organizations, proxies)
    levels_fr, levels_it, levels_de, levels_en = slug_federal_level(
        organizations, df_slug_toplevel_names, proxies
    )
    names_fr, names_it, names_de, names_en = slug_names(
        slugs_packages_dict, df_slug_toplevel_names, organizations
    )
    dataframe = create_dataframe(
        organizations,
        nr_packages,
        names_fr,
        names_it,
        names_de,
        names_en,
        levels_fr,
        levels_it,
        levels_de,
        levels_en,
    )
    df_top_report, df_dep_report, df_fed_report = create_csv_from_dataframe(dataframe)
    results = fetch_packages(TEST, proxies)
    max_resources, total_resources = get_max_resources(results)
    geocat = from_geocat(results)
    create_report(
        organizations,
        df_top_report,
        df_dep_report,
        df_fed_report,
        listed_organizations,
        nr_packages,
        max_resources,
        total_resources,
        geocat,
    )
    end = time.perf_counter()
    print(f"{end-start} total runtime")


if __name__ == "__main__":
    main(BUND, TEST)
