import pyodbc
import pandas as pd
from sqlalchemy import create_engine

# IPython and Jupyter Notebooks
# https://www.datacamp.com/community/blog/ipython-jupyter#gs.rfRCfjw
# https://www.packtpub.com/books/content/getting-started-jupyter-notebook-part-2

# Jupyter features
# http://arogozhnikov.github.io/2016/09/10/jupyter-features.html

# Pandas link
# https://github.com/pandas-dev/pandas/tree/master/doc/cheatsheet/Pandas_Cheat_Sheet.pdf
# http://pandas.pydata.org/pandas-docs/stable/index.html
# http://pandas.pydata.org/pandas-docs/stable/
# http://pandas.pydata.org/pandas-docs/stable/io.html#sql-queries

# https://github.com/mkleehammer/pyodbc/wiki/Getting-started
# http://docs.sqlalchemy.org/en/latest/core/engines.html

# Python
# https://www.python.org/dev/peps/pep-0008/
# http://python-guide-pt-br.readthedocs.io/en/latest/writing/gotchas/

# Pandas
# https://www.dataquest.io/blog/images/cheat-sheets/pandas-cheat-sheet.pdf

def get_plan_color_and_size_percents(reference_item_division_codes = None, department_codes = None,
                                     reference_item_codes = None,
                                     media_division_codes = None, season_codes = None, media_codes = None):

    # region Default Parameter Boilerplate
    if reference_item_division_codes is None:
        reference_item_division_codes = []
    if department_codes is None:
        department_codes = []
    if reference_item_codes is None:
        reference_item_codes = []
    if media_division_codes is None:
        media_division_codes = []
    if season_codes is None:
        season_codes = []
    if media_codes is None:
        media_codes = []
    # endregion

    engine = create_engine("mssql+pyodbc://icadmin:adm1n@localhost:1433/BAG_IPM?driver=ODBC+Driver+13+for+SQL+Server")

    # load the media info first
    if not media_codes:

        if not media_division_codes:
            #media_division_info = pd.read_sql("SELECT div.Division_key, div.Division_code FROM Division div", engine)
            media_division_info = pd.read_sql("SELECT div.Division_key, div.Division_code FROM Division div", engine)
        else:
            media_division_info = pd.read_sql("SELECT div.Division_key, div.Division_code " +
                                              "FROM Division div WHERE div.Division_code IN (" + ','.join(map(str, media_division_codes)) + ")",
                                              engine)

        if not season_codes:
            season_info = pd.read_sql("SELECT seas.Season_key, seas.Season_code FROM Season seas", engine)
        else:
            season_info = pd.read_sql("SELECT seas.Season_key, seas.Season_code " +
                                      "FROM Season seas " +
                                      "WHERE seas.Season_code IN (" + ','.join(map(str, season_codes)) + ")",
                                      engine)

        media_info = pd.read_sql("SELECT m.Media_key, m.Media_code " +
                                 "FROM Media m " +
                                 "WHERE m.Season_fkey   IN (" + ','.join(map(str, season_info["Season_key"])) + ")" +
                                 "AND   m.Division_fkey IN (" + ','.join(map(str, media_division_info["Division_key"])) + ")",
                                 engine)
    else:
        media_info = pd.read_sql("SELECT m.Media_key, m.Media_code FROM Media m where m.Media_code IN (" + ','.join(map(str, media_codes)) + ")", engine)


    # now load the reference item info
    if not reference_item_codes:

        if not reference_item_division_codes:
            reference_item_division_info = pd.read_sql("SELECT div.Division_key, div.Division_code FROM Division div", engine)
        else:
            reference_item_division_info = pd.read_sql("SELECT div.Division_key, div.Division_code " +
                                                       "FROM Division div WHERE div.Division_code IN (" + ','.join(map(str, reference_item_division_codes)) + ")",
                                                       engine)
        if not department_codes:
            department_info = pd.read_sql("SELECT dep.Department_key, dep.Department_code FROM Department dep", engine)
        else:
            department_info = pd.read_sql("SELECT dep.Department_key, dep.Department_code " +
                                          "FROM Department dep WHERE dep.Department_code IN (" + ','.join(map(str, department_codes)) + ")",
                                          engine)

        reference_item_info = pd.read_sql("SELECT ri.ReferenceItem_key, ri.Reference_code AS ReferenceItem_code, " +
                                          "ri.Department_fkey as Department_key, ri.Division_fkey as Division_fkey " +
                                          "FROM ReferenceItem ri " +
                                          "WHERE ri.Department_fkey IN (" + ','.join(map(str, department_info["Department_key"])) + ")" +
                                          "AND   ri.Division_fkey   IN (" + ','.join(map(str, reference_item_division_info["Division_key"])) + ")",
                                          engine)

    else:
        reference_item_info = pd.read_sql("SELECT ri.ReferenceItem_key, ri.Reference_code AS ReferenceItem_code, " +
                                                       "ri.Department_fkey as Department_key, ri.Division_fkey as Division_fkey " +
                                                       "FROM ReferenceItem ri " +
                                                       "WHERE ri.Reference_code IN (" + ','.join(map(str, reference_item_codes)) + ")",
                                                       engine)

    # now that we have the media and reference item info we can load the color and size plans
    color_plans = pd.read_sql("""SELECT iap.ItemAssortmentPlan_key,iap.ReferenceItem_fkey, iap.Media_fkey,
                                                  iap.PlanType_fkey, pt.Name as PlanTypeName,
                                                  iap.UseActualColorPercentages,
                                                  iap.Deleted as IAPDeleted, cpe.Deleted as CPEDeleted,
                                                  cpe.ColorPlan_fkey, cpe.SizeClass_fkey,
                                                  cpe.Color_fkey, cpe.PlannedPercentDemand, cpe.ColorPlanEntry_key,
                                                  c.Description as ColorName, c.Color_code
                                           FROM ItemAssortmentPlan iap
                                           INNER JOIN PlanType pt ON pt.PlanType_key = iap.PlanType_fkey
                                           INNER JOIN ColorPlanEntry cpe on cpe.ColorPlan_fkey = iap.ColorPlan_fkey
                                           INNER JOIN Color c on c.Color_key = cpe.Color_fkey """ +
                                           "WHERE  iap.ReferenceItem_fkey in (" + ','.join(map(str, reference_item_info["ReferenceItem_key"]))+ ")" +
                                           "AND  iap.Media_fkey in (" + ','.join(map(str, media_info["Media_key"]))+ ")" +
                                           """ORDER BY iap.ItemAssortmentPlan_key, cpe.ColorPlan_fkey, cpe.SizeClass_fkey""",
                                           engine)

    size_plans = pd.read_sql("""SELECT iap.ItemAssortmentPlan_key,iap.ReferenceItem_fkey, iap.Media_fkey,
                                                  iap.PlanType_fkey, pt.Name as PlanTypeName,
                                                  iap.UseActualSizePercentages,
                                                  iap.Deleted as IAPDeleted, spe.Deleted as SPEDeleted,
                                                  spe.SizePlan_fkey,
                                                  spe.Size_fkey, spe.PlannedPercentDemand, spe.SizePlanEntry_key,
                                                  sz.Size_code as SizeCode, sz.SizeClass_fkey
                                           FROM ItemAssortmentPlan iap
                                           INNER JOIN PlanType pt ON pt.PlanType_key = iap.PlanType_fkey
                                           INNER JOIN SizePlanEntry spe on spe.SizePlan_fkey = iap.SizePlan_fkey
                                           INNER JOIN Size sz on sz.Size_key = spe.Size_fkey """ +
                              "WHERE  iap.ReferenceItem_fkey in (" + ','.join(map(str, reference_item_info["ReferenceItem_key"]))+ ")" +
                              "AND  iap.Media_fkey in (" + ','.join(map(str, media_info["Media_key"]))+ ")" +
                              """ORDER BY iap.ItemAssortmentPlan_key, spe.SizePlan_fkey, sz.SizeClass_fkey""",
                              engine)

    shipped_by_ref_item_and_media = pd.read_sql("""SELECT
                                                        i.ReferenceItem_fkey,
                                                        i.Item_key,
                                                        s.SKU_key,
                                                        sh.Media_fkey,
                                                        SUM(sh.UnitCount) as TotalShippedUnitsPerMedia
                                                    FROM Shipped sh
                                                    INNER JOIN SKU s ON s.SKU_key = sh.SKU_fkey
                                                    INNER JOIN Item i ON i.Item_key = s.Item_fkey
                                                    INNER JOIN Color c ON c.Color_key = s.Color_fkey
                                                    INNER JOIN Size sz ON sz.Size_key = s.Size_fkey """ +
                                                    "WHERE  i.ReferenceItem_fkey in (" + ','.join(map(str, reference_item_info["ReferenceItem_key"]))+ ")" +
                                                    "AND  sh.Media_fkey in (" + ','.join(map(str, media_info["Media_key"]))+ ")" +
                                                    """GROUP BY i.ReferenceItem_fkey, i.Item_key, s.SKU_key, sh.Media_fkey""",
                                                engine);

    return size_plans



#engine = create_engine("mssql+pyodbc://icadmin:adm1n@localhost:1433/BAG_IPM?driver=SQL+Server+Native+Client+10.0")
engine = create_engine("mssql+pyodbc://icadmin:adm1n@localhost:1433/BAG_IPM?driver=ODBC+Driver+13+for+SQL+Server")
x = pd.read_sql_table('Color', engine)
#print(x)
x = pd.read_sql("Select top 10 * from Size", engine)
#print(x)
x = get_plan_color_and_size_percents(reference_item_division_codes={3,4})
print(x)




