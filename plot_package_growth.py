import requests
from datetime import datetime, timedelta
import urllib3
import pandas as pd


def run_plot_package_growth():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # prepare DataFrame
    #### prepare variables and define requested intervall to be displayed

    proxies = None
    s = requests.Session()

    now = datetime.now()
    now = now.strftime("%d-%m-%Y %H:%M:%S")

    #### request.get Organizations
    organizations = s.get(
        "https://ckan.opendata.swiss/api/3/action/organization_list",
        proxies=proxies,
        verify=False,
    ).json()["result"]

    base_URL_org = (
        "https://ckan.opendata.swiss/api/3/action/package_search?fq=organization:"
    )

    #### Run the query
    df_organizations = pd.DataFrame({"name": [], "joined": []})

    for organization in range(len(organizations)):
        try:  # certain organizations do not have datasets, i.e. only their suborganisations have such
            name = organizations[organization]
            joined = s.get(base_URL_org + name, proxies=proxies, verify=False).json()[
                "result"
            ]["results"][0]["organization"]["created"]
            nr_packages = s.get(
                base_URL_org + name, proxies=proxies, verify=False
            ).json()["result"]["count"]
            df2 = pd.DataFrame(
                {"name": [name], "joined": [joined], "nr_packages": [nr_packages]}
            )
            df2["joined"] = pd.to_datetime(df2["joined"])
            df_organizations = pd.concat([df_organizations, df2], ignore_index=True)
        except:
            continue

    # packages['issued'] = pd.to_datetime(packages['issued'])
    # df_organizations = df_organizations["organization"]
    # print(df_organizations.head())
    """
    df_2 = pd.DataFrame()
    for nr, row in df_organizations:
        df_2 = df_2.append(pd.read_json(row))

    print(df_2.head())

    """

    df_organizations.sort_values("joined", ascending=True, inplace=True)
    # df_organizations = df_organizations.drop_duplicates(subset="package_id")
    df_organizations = df_organizations.reset_index(drop=True)

    # df_organizations["name"] = df_resources["name"].to_string()
    df_organizations["Total"] = 1
    df_organizations["Total"] = df_organizations["Total"].astype(int)
    df_organizations["Total"] = [
        index + 1 for index, row in df_organizations.iterrows()
    ]

    plot = df_organizations.plot(
        x="joined",
        y="Total",
        figsize=(12, 6),
        title=f"Publication of current datasets on opendata.swiss by {now}",
    )

    fig = plot.get_figure()
    fig.savefig("output.png")

    # SAFE PLOT TO FILE
    # plot.savefig("packages_plot.png")


if __name__ == "__main__":
    run_plot_package_growth()
