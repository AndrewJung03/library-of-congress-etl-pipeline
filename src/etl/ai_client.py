import pandas as pd
from google import genai
from dotenv import load_dotenv
import os

load_dotenv()
client = genai.Client()

CLEANED_CSV = "data/cleaned/newspapers_cleaned.csv"

SYSTEM_PROMPT = """
You are a data analysis assistant.

You have access to a dataset of cleaned historical newspaper issues (already loaded below).
The user will ask questions about trends, patterns, counts, or insights.

Your job:
- Analyze the dataset provided.
- Provide clear, concise explanations.
- Do NOT generate SQL.
- Do NOT hallucinate columns. Only use what exists in the dataset.
- If a question requires calculation, do it using the dataset contents included in your prompt.
"""


def load_dataset_summary(df: pd.DataFrame) -> str:
    """
    Converts the dataframe into a compact text summary that Gemini can reason over.
    Not the full dataset, but a compressed slice + schema.
    """
    info = []

    info.append("COLUMNS:\n" + ", ".join(df.columns) + "\n")

    # Show sample rows so the model understands the structure
    sample = df.head(15).to_string(index=False)
    info.append("SAMPLE ROWS:\n" + sample + "\n")

    # Basic stats
    info.append("DATASET SIZE: " + str(len(df)) + " rows\n")

    return "\n".join(info)


def ask_ai(question: str, df: pd.DataFrame) -> str:
    """
    Sends dataset context + question to Gemini and returns the answer.
    """
    dataset_context = load_dataset_summary(df)

    prompt = (
        SYSTEM_PROMPT
        + "\n\nDATASET CONTEXT:\n"
        + dataset_context
        + "\n\nUSER QUESTION: "
        + question
    )

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    return response.text.strip()


def start_ai_cli():
    print("AI Data Insights Assistant Started")
    print("Dataset loaded from:", CLEANED_CSV)
    print("Type 'exit' to quit.\n")

    df = pd.read_csv(CLEANED_CSV)

    while True:
        user_q = input("Ask a data question: ")

        if user_q.lower() in ["exit", "quit"]:
            break

        try:
            answer = ask_ai(user_q, df)
            print("\n=== AI INSIGHT ===")
            print(answer)
            print("==================\n")

        except Exception as e:
            print("Error:", e)


if __name__ == "__main__":
    start_ai_cli()
