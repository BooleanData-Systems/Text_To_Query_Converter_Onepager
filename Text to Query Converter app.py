import streamlit as st
from snowflake.snowpark import Session
import pandas as pd

# -----------------------
# Streamlit Config
# -----------------------
st.set_page_config(page_title="Snowflake Text To SQL Converter", layout="wide")

# st.title("❄️Text To SQL Convertert")
st.title("Text To Query Builder")

with st.sidebar:
    # Logo
    st.image(
        "https://booleandata.ai/wp-content/uploads/2022/09/Boolean-logo_Boolean-logo-USA-1.png",
        use_container_width=True
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # # Model accuracy
    # st.subheader("📊 Model Accuracy")
    # st.metric("Hold-out Accuracy", f"{holdout_acc:.2%}")

    # Spacer
    st.markdown("<br><br><br><br><br>", unsafe_allow_html=True)

    # About Us
    st.markdown("""
<div style="font-size:15px; line-height:1.4; color:#333;text-align: center;">
<h5 style="font-size:18px;">🚀 About Us</h5>
We leverage Snowflake to plan and design emerging data architectures that facilitate incorporation of high-quality and flexible data. 
<br><br>
These solutions lower costs and enhance output, designed to transform smoothly as your enterprise, and your data continue to increase over time.
</div>
    """, unsafe_allow_html=True)

    # Social media links
    st.markdown("""
<div style="text-align:center; display:flex; justify-content:center; gap:15px; margin-top:10px;">
<a href="https://booleandata.ai/" target="_blank">🌐</a>
<a href="https://www.facebook.com/Booleandata" target="_blank">
<img src="https://cdn-icons-png.flaticon.com/24/1384/1384005.png" width="24">
</a>
<a href="https://www.youtube.com/channel/UCd4PC27NqQL5v9-1jvwKE2w" target="_blank">
<img src="https://cdn-icons-png.flaticon.com/24/1384/1384060.png" width="24">
</a>
<a href="https://www.linkedin.com/company/boolean-data-systems" target="_blank">
<img src="https://cdn-icons-png.flaticon.com/24/145/145807.png" width="24">
</a>
</div>
    """, unsafe_allow_html=True)

# -----------------------
# Snowflake Session (built-in, no manual connection needed in Snowflake-hosted Streamlit)
# -----------------------
session = Session.builder.appName("CortexSQLChatbot").getOrCreate()

# -----------------------
# Database & Schema Selectors
# -----------------------
databases = [db['name'] for db in session.sql("SHOW DATABASES").collect()]
selected_db = st.selectbox("Select Database", databases)

schemas = [s['name'] for s in session.sql(f"SHOW SCHEMAS IN DATABASE {selected_db}").collect()]
selected_schema = st.selectbox("Select Schema", schemas)

# -----------------------
# Build Metadata Context (Tables + Columns)
# -----------------------
tables = session.sql(f"SHOW TABLES IN SCHEMA {selected_db}.{selected_schema}").collect()
schema_context = ""

for t in tables:
    table_name = t['name']
    cols = session.sql(
        f"SHOW COLUMNS IN TABLE {selected_db}.{selected_schema}.{table_name}"
    ).collect()
    col_names = [c['column_name'] for c in cols]
    schema_context += f"\nTable: {selected_db}.{selected_schema}.{table_name}, Columns: {', '.join(col_names)}"

# -----------------------
# User Question Input
# -----------------------
question = st.text_area("Enter The Text ", "")

# -----------------------
# Get Answer Button
# -----------------------
if st.button("Generate Query"):
    if not question.strip():
        st.warning("Please enter a question!")
    else:
        try:
            # Cortex Prompt with full schema context
            cortex_prompt = f"""
            You are an expert Snowflake SQL generator.
            The user is asking a question in plain English.
            
            Context:
            - Database: {selected_db}
            - Schema: {selected_schema}
            - Available tables and columns: {schema_context}
            
            Rules:
            1. Only include columns explicitly required to answer the user’s question.
            2. Do not add any extra columns, even if joins are required.
            3. Include join keys only if strictly necessary to make the query run.
            4. Always use fully qualified table names: {selected_db}.{selected_schema}.<table>.
            5. Return only a valid SQL query. No explanations, comments, or markdown.
            6. Always format the SQL query with proper indentation and line breaks (multi-line, human-readable).
            7. Do not add JOINs if all requested columns exist in a single table. Use joins only when columns from multiple tables are required.
            8. Always break SQL into multiple lines.
               - Place SELECT on its own line.
               - Put each selected column on a separate new line.
               - Place FROM on its own line.
               - Each JOIN on its own line.
               - WHERE, GROUP BY, ORDER BY each on their own line.
               - Never return SQL in a single line, even for short queries.

            Question: {question}
            """


            
            # Call Cortex COMPLETE
            sql_gen = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'snowflake-arctic',
                '{cortex_prompt.replace("'", "''")}'
            ) AS GENERATED_SQL
            """

            sql_result = session.sql(sql_gen).collect()
            generated_sql = sql_result[0]['GENERATED_SQL'].strip()

            st.subheader("📝 Generated SQL")
            st.code(generated_sql, language="sql")



        except Exception as e:
            st.error(f"Cortex error: {str(e)}")
