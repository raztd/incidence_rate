from sqlite3 import Error, connect
from pandas import read_excel, DataFrame


excel_file = "transparenta_29_martie_2021.xlsx"
# path to folder where the excel file is located
# & where .csv & .sqlite3 fieles will be saved
working_dir = "./"
columns_to_keep = ["Judet", "UAT", "2021-03-29"]
table_name = "incidenta"

'''
script to analyze "transparenta covid-19" [0] data
prerequirements: python 3.7+, pandas, openpyxl
[0] https://data.gov.ro/dataset/transparenta-covid

by Răzvan T Duca

bitcoin donations: bc1q5f5km4x2etmylthjt87jn8j09gtwx4vyjewwhg

contact: razvan.t.duca@protonmail.ch

-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: OpenPGP.js v4.10.8
Comment: https://openpgpjs.org

xsBNBFpABjIBCADK/+BWrVn7Qgqu96ThKmaKN762cRJSwy9jGZ4Y9ODgu+0B
G10g0PsS+sm5az6XmgdWlcPNUih6fgCR9p+rAxpHAa1fHOuDZLulH2PRlBhN
5BALNM644wiHhzkjVxCewHuaW6eyu5ktZml0WSML/6R3La1a8PM31voQlgsP
4ELTwjLl0xZ3c3nbfrFmO2Q8HMSQQrzWB7sc78pUm56tzGKd9BVsSxGG3FuJ
AS3oYhT9DlqXGteU6v0gle6S2ut3WeWbeTaZWBu4VV5Uidm91brZbtzGEVU1
+nNJGULVoXkjiCtOu8LaYd0Xg55NKf8KIFjm4UhjWQQlb2MdBN3JhbSJABEB
AAHNOXJhenZhbi50LmR1Y2FAcHJvdG9ubWFpbC5jaCA8cmF6dmFuLnQuZHVj
YUBwcm90b25tYWlsLmNoPsLAfwQQAQgAKQUCWkAGMwYLCQcIAwIJECwrXYgU
eBrdBBUICgIDFgIBAhkBAhsDAh4BAAoJECwrXYgUeBrdzfMH/RELh+IppI9t
2pFRTXQ4/5+WbEff4eVvNEK5A5fm0CFZdo2z1qDCJF58dBFIVYnkXkhJ9jgm
qX47WdqORmGR5brEVIzbqsAQGnCoZFAe8rO/n1aj981lOJYrHNTi3lKqEsum
rY28vS3eUEVn7AvXQKcFbpz/ZBtam5Fh4XsqgVQtz3dKuAMNpUTWPwHS4lTz
P+UxnJsxTf5jJeUCzvB3/PO4ZPNYj7TWA85BF0Ryj9bauDu2yk7Rtwpo4FQZ
c8cqg6Kwj+fmi4iVijkyC/5kUIO9971dueCpY2CY8nMY3Sy+RB5g58eF+GiD
y9z5oyUlBFuj6ITWjTAzta67RmzhIpTOwE0EWkAGMgEIANTP2y++J+fqptWc
2++mngsRyPFl6a5RwGgXGliVenh3/oAQuOEQ3vB73X7PVpNDdKgvSSeTGStv
YRZWPUtmc/BzG+1neBV+3PKrTIahQZsUU/Xc3BPRxK6XlRu6h8IcbQQOpG1R
5UjQ+El1uEtrkS4rdTsxHhcu74iNcHfVI5/uebjYgwZl+RQpni6Wb5COVI7p
8YmBO1c5u+fWPUh4GizJ4QUt7BYgm//raZqCln1vqEhkSu0BctF7EeuuU4jF
AKaa1fBAc7yrFa2qQ78QEQ04al8k73lTDZkD4TLoWuPOdfZ7gFXyNQKkNe6d
0DeO/zC4Ea1+XVaHYlqgRDlqF4UAEQEAAcLAaQQYAQgAEwUCWkAGMwkQLCtd
iBR4Gt0CGwwACgkQLCtdiBR4Gt1Ohgf/SjEqJKRw2hqRoNol9pbcHBN71PzH
weaV+koqNfEx2MAYrgsD+9Wptl3E1WRKF7lDxUPb7G57ypvKmlvVeMYs+0md
sUxf/C191kK0oaXIeU02TyowrGTn1b9U+MFQOPklZ9aFu8NghAZALYriyiGL
UwJsdRfRTlno02UeCNjtbLVd/74Cp7RU14mVqtc2oLHbe47bnRStpLBM9Yhu
2Nmy4+U8EiD9VC1tJZlbIn9MEoY4N2NVoUJOHgJwoYOIlsmBB4UbJ1Ad0vMw
5xRqLOkVDgsmod2kvAmoSBXm9C1EvUQjYgy++NTv+EMVztVRjf5Q+DdpRy1i
OfL+E98rab3bYg==
=7VR1
-----END PGP PUBLIC KEY BLOCK-----
'''


def get_data(filepath):
    return read_excel(filepath, header=2)


def filter_data(df, columns_to_keep):
    return df[columns_to_keep]


class Db:
    def __init__(self, filepath):
        try:
            self.__con = connect(filepath)
        except Error as e:
            print(e)

    def save_df(self, df, table_name):
        df.to_sql(table_name, self.__con, index=False)


def unique_values_from_column(df, column):
    return df[column].dropna().unique().tolist()


def save_aux_info_to_file(df):
    counties = unique_values_from_column(df, "judet")
    with open("counties_and_uat.txt", "w", encoding='utf-8') as file:
        for county in counties:
            print(f"({county})", file=file)
            df2 = df[df["judet"] == county]
            print(unique_values_from_column(df2, "uat"), file=file)
            print("", file=file)


def run():
    df = get_data(f"{working_dir}{excel_file}")
    df = filter_data(df, columns_to_keep)
    # change to lowercase
    df.rename(columns=str.lower, inplace=True)
    df["judet"] = df["judet"].map(str.lower)
    df["uat"] = df["uat"].map(str.lower)
    # replace diacritics
    dictionary = {"ă": "a", "â": "a", "î": "i", "ş": "s", "ţ": "t"}
    df.replace(dictionary, regex=True, inplace=True)

    save_aux_info_to_file(df)
    db = Db(f"{working_dir}{excel_file}.sqlite3")
    db.save_df(df, table_name)


if __name__ == "__main__":
    run()
