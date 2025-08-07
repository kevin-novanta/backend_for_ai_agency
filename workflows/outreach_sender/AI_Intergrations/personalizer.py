
from workflows.outreach_sender.Email_Scripts.email_scripts import get_opener_prompt




import json
import openai
import os

with open("/Users/kevinnovanta/backend_for_ai_agency/Creds/gpt_key.json") as f:
    secrets = json.load(f)
openai.api_key = secrets["OPENAI_API_KEY"]


def generate_personalized_email(lead):
    company_name = lead.get("Company Name", "").strip()
    offer_summary = lead.get("Custom 2", "").strip()

    prompt = get_opener_prompt(company_name, offer_summary)

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=200,
        )

        output = response.choices[0].message.content.strip()
        lines = output.split("\n")
        subject = ""
        body = ""

        for line in lines:
            if line.lower().startswith("subject:"):
                subject = line.split(":", 1)[1].strip()
            elif line.lower().startswith("body:"):
                body = line.split(":", 1)[1].strip()
            else:
                body += "\n" + line.strip()

        return subject, body.strip()

    except Exception as e:
        print(f"❌ Error generating personalized email: {e}")
        return "Quick Question", "Hey – just came across your company and had an idea. Mind if I share?"