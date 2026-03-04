import os
import requests
import json
import mysql.connector
from dotenv import load_dotenv
import schedule
import time

# ===============================
# LOAD ENVIRONMENT VARIABLES
# ===============================
load_dotenv()

# ===============================
# GITHUB CONFIG
# ===============================
GRAPHQL_URL = "https://github.sec.samsung.net/api/graphql"
GITHUB_TOKEN = os.getenv("GITHUB_API_KEY")
ORG_NAME = "METO"
PROJECT_NUMBER = 3
OUTPUT_FILE = "meto_project3.json"

HEADERS = {
    "Authorization": f"bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}

# ===============================
# MYSQL CONFIG
# ===============================
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

DB_TABLE = "github_projects"

# ===============================
# GRAPHQL HELPERS
# ===============================
def run_query(query, variables=None):
    response = requests.post(
        GRAPHQL_URL,
        headers=HEADERS,
        json={"query": query, "variables": variables or {}}
    )

    if response.status_code != 200:
        raise Exception(response.text)

    result = response.json()

    if "errors" in result:
        raise Exception(json.dumps(result["errors"], indent=2))

    return result

# ===============================
# FETCH PROJECT ITEMS
# ===============================
def fetch_project_items(org, project_number):
    query = """
    query($org: String!, $number: Int!, $after: String) {
      organization(login: $org) {
        projectV2(number: $number) {
          items(first: 50, after: $after) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              content {
                ... on Issue {
                  title
                  url
                }
                ... on PullRequest {
                  title
                  url
                }
              }
              fieldValues(first: 30) {
                nodes {
                  ... on ProjectV2ItemFieldTextValue {
                    text
                    field {
                      ... on ProjectV2FieldCommon {
                        name
                      }
                    }
                  }
                  ... on ProjectV2ItemFieldSingleSelectValue {
                    name
                    field {
                      ... on ProjectV2FieldCommon {
                        name
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    """

    all_items = []
    after_cursor = None

    while True:
        variables = {
            "org": org,
            "number": project_number,
            "after": after_cursor
        }

        result = run_query(query, variables)
        items = result["data"]["organization"]["projectV2"]["items"]

        all_items.extend(items["nodes"])

        if items["pageInfo"]["hasNextPage"]:
            after_cursor = items["pageInfo"]["endCursor"]
        else:
            break

    print(f"✅ Fetched {len(all_items)} GitHub Project items")
    return all_items

# ===============================
# TRANSFORM DATA
# ===============================
def transform_items(items):
    transformed = []

    for item in items:
        row = {
            "Title": None,
            "Title URL": None,
            "Stages": None,
            "Country": None,
            "Operator": None,
            "Category": None,
            "SUB": None,
            "Email Title": None
        }

        content = item.get("content")
        if content:
            row["Title"] = content.get("title", "")
            row["Title URL"] = content.get("url", "")

        for field in item.get("fieldValues", {}).get("nodes", []):
            field_name = field.get("field", {}).get("name", "")
            value = field.get("text") or field.get("name")

            if field_name in row:
                row[field_name] = value

        transformed.append(row)

    return transformed

# ===============================
# SAVE JSON (OPTIONAL DEBUG)
# ===============================
def save_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"💾 Saved JSON → {filename}")

# ===============================
# WRITE TO MYSQL
# ===============================
def write_to_mysql(data):
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

    cursor = conn.cursor()

    inserted = 0

    for item in data:
        if not item["Email Title"]:
            continue

        sql = f"""
        INSERT INTO {DB_TABLE}
        (title, url, stages, country, operator, category, sub, email_title)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            url = VALUES(url),
            stages = VALUES(stages),
            country = VALUES(country),
            operator = VALUES(operator),
            category = VALUES(category),
            sub = VALUES(sub)
        """

        cursor.execute(sql, (
            item["Title"],
            item["Title URL"],
            item["Stages"],
            item["Country"],
            item["Operator"],
            item["Category"],
            item["SUB"],
            item["Email Title"]
        ))

        inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted / Updated {inserted} rows in `{DB_TABLE}`")

# ===============================
# MAIN JOB
# ===============================
def job():
    print("🕒 Running GitHub → MySQL sync")
    items = fetch_project_items(ORG_NAME, PROJECT_NUMBER)
    transformed = transform_items(items)
    save_json(transformed, OUTPUT_FILE)
    write_to_mysql(transformed)

# ===============================
# SCHEDULER
# ===============================
schedule.every().day.at("07:13").do(job)

print("📅 Scheduler started (daily at 07:13)")
while True:
    schedule.run_pending()
    time.sleep(60)
