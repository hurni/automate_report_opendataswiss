from bokeh.layouts import column
from bokeh.models import (
    ColumnDataSource,
    DataTable,
    HoverTool,
    StringFormatter,
    TableColumn,
    DateFormatter,
    Quad,
)
from bokeh.plotting import figure, show, save, output_file
import requests
from datetime import datetime
import urllib3
import pandas as pd


def run_new_org_table():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # prepare DataFrame

    #### prepare variables and define requested intervall to be displayed

    proxies = None

    s = requests.Session()

    now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

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
    df = pd.DataFrame({"name": [], "joined": []})

    for organization in range(len(organizations)):
        try:  # certain organizations do not have datasets, i.e. only their suborganisations have such
            name = organizations[organization]
            joined = s.get(base_URL_org + name, proxies=proxies, verify=False).json()[
                "result"
            ]["results"][0]["organization"]["created"]
            df2 = pd.DataFrame({"name": [name], "joined": [joined]})
            df2["joined"] = pd.to_datetime(df2["joined"])
            df = pd.concat([df, df2], ignore_index=True)
        except:
            continue

    # order dataframe as required and set dataformat in 'joined' as datetime
    df.sort_values("joined", ascending=True, inplace=True)
    df = df.reset_index(drop=True)
    df["joined"] = pd.to_datetime(df["joined"])
    df["Total"] = [index + 1 for index, row in df.iterrows()]

    df_joined = df

    # print(df_joined)

    # prepare visualization
    source = ColumnDataSource(df_joined)

    organizations = sorted(df_joined["name"].unique())
    joined = sorted(df_joined["joined"].unique())

    columns = [
        TableColumn(
            field="name",
            title="Organizations",
            width=800,
            formatter=StringFormatter(font_style="bold"),
        ),
        TableColumn(
            field="joined",
            title="joined",
            default_sort="descending",
            width=200,
            formatter=DateFormatter(format="%d/%m/%Y"),
        ),
    ]

    data_table = DataTable(
        source=source,
        columns=columns,
        index_position=-1,
        index_header="Total",
        index_width=60,
        autosize_mode="fit_columns",
        sizing_mode="stretch_width",
    )

    p = figure(
        width=800,
        height=300,
        tools="tap,pan,wheel_zoom,xbox_select,reset",
        active_drag="xbox_select",
        x_axis_type="datetime",
        x_axis_label="joined",
        y_axis_label="Total Organizations on opendata.swiss",
        title=f"Organizations on opendata.swiss, last update {now}",
    )

    org = p.circle(
        x="joined",
        y="Total",
        fill_color="#000000",
        size=8,
        alpha=0.7,
        source=source,
        legend_label="Organizations Growth on opendata.swiss",
    )
    p.line(
        x="joined",
        y="Total",
        line_width=2,
        line_color="#009688",
        alpha=0.3,
        source=source,
    )
    glyph = Quad(
        bottom=0,
        left="joined",
        right="joined",
        top="Total",
        fill_color="#009688",
        fill_alpha=0.7,
        line_color="#009688",
        line_alpha=0.2,
    )
    p.add_glyph(source, glyph)

    hover_tool = HoverTool(
        renderers=[org],
        tooltips=[("Organization", "@name"), ("joined", "@joined{%d/%m/%Y}")],
        formatters={"@joined": "datetime"},
    )

    p.legend.location = "top_left"
    band = p.varea(
        source=source,
        x="joined",
        y1=0,
        y2="Total",
        alpha=0.2,
        fill_color="#009688",
        legend_label="Organizations on opendata.swiss",
    )

    p.add_layout(band)
    p.add_tools(hover_tool)

    output_file("new_org_table.html", title="Organizations on opendata.swiss")

    # show(column(p, data_table))
    save(column(p, data_table))


if __name__ == "__main__":
    run_new_org_table()
