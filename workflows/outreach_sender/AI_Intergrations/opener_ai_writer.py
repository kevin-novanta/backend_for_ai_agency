import json
from workflows.outreach_sender.Email_Scripts.email_scripts import get_opener_prompt
from openai import OpenAI

# Load your OpenAI API key from JSON file
with open("Creds/gpt_key.json") as f:
    openai_key = json.load(f)["api_key"]

client = OpenAI(api_key=openai_key)

def build_prompt(lead):
    company_name = lead.get("Company Name", "Unknown Company")
    industry_info = lead.get("Custom 2", "Unknown Industry")
    offer_summary = lead.get("Overview", "provide services")

    prompt = f"""
### Company Info:
- Name: {company_name}
- Industry: {industry_info}
- Offer Summary: {offer_summary}

### Your Task:
Write a short, warm cold outreach email (under 120 words) to the company above.

### Script Rules:
1. Do NOT summarize what the company does. Instead, use their offer to show deep understanding and context.
2. Mention our company — Outbound Accelerator — as specialists in advanced AI workflows.
3. Say: "We help companies like yours — companies that {offer_summary.lower()} — streamline complex operations and drive results."
4. Mention tailored-specific results: reduce manual tasks, eliminate inefficiencies, and increase booked calls by 2–3x. (This will not be in the email, but is for your context.)
5. Also include 1–2 of these 2025-relevant benefits of AI:
   - Smarter, faster decisions
   - Scaling without hiring
   - Personalized experiences
   - Automated compliance/risk tracking
   - Refocusing team on high-value work
6. Also highlight 1–2 common pain points solved:
   - Human error from messy workflows
   - Tool overload / siloed systems
   - Workload overload (e.g., 1 person juggling 50+ clients)
   - No automation or optimization support
   - Old systems blocking full-funnel automation
7. Be specific with industry context: instead of “businesses like yours,” say “other {offer_summary.lower()} businesses.”
8. Close by naming a likely pain (complexity, error, etc.) and why a tailored workflow would help.
9. End the email inviting them to watch a short video breakdown showing how our system works and how it applies to them.
10. Final sentence must include our company name: Outbound Accelerator.
11. This email must be short, warm, and human-sounding. Avoid sounding generic or robotic. 

### Example Output (for style/tone only):
"At Outbound Accelerator, we help online learning companies reduce backend tasks and book more calls with AI-powered workflows tailored to their process."

### Output Format:
- Keep it casual, friendly, human-sounding
- No emojis, fake flattery, square brackets, or robotic tone
"""
    return prompt

def generate_email(lead):
    """
    Sends a prompt to OpenAI and returns the subject and body_html.
    """
    prompt = build_prompt(lead)

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a B2B cold email generator."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        content = response.choices[0].message.content

        try:
            email = json.loads(content)
        except Exception:
            email = eval(content.strip())
        return email

    except Exception as e:
        print(f"❌ Error generating email: {e}")
        return {"subject": "Follow up", "body_html": "Hi – just following up. Let me know if you're interested."}

def generate_email_from_prompt(prompt, openai_key):
    """
    Sends a custom prompt to OpenAI and returns the subject and body_html.
    """
    local_client = OpenAI(api_key=openai_key)

    try:
        print(f"📨 Prompt being sent:\n{prompt}")
        response = local_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a B2B cold email generator."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        content = response.choices[0].message.content

        try:
            email = json.loads(content)
        except Exception:
            print(f"⚠️ Raw OpenAI response content:\n{content}")
            email = {"subject": "Follow up", "body_html": content.strip()}

        return email

    except Exception as e:
        print(f"❌ Error generating email from prompt: {e}")
        return {"subject": "Follow up", "body_html": "Hi – just following up. Let me know if you're interested."}
