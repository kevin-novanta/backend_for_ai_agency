import sys
import os
# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


import json
from pathlib import Path
from workflows.outreach_sender.AI_Intergrations.opener_ai_writer import generate_email_from_prompt

def load_openai_key():
    creds_path = Path(__file__).resolve().parents[2] / "Creds" / "gpt_key.json"
    with open(creds_path, "r") as file:
        creds = json.load(file)
    return creds["api_key"]

def run_prompt_test():
    company_name = "Purposeful Agency"
    industry_info = "Purposeful Agency is an agency specializing in online learning and video production. For outstanding work in Miami Call: 415 408 8549."
    offer_summary = "Purposeful Agency is an agency specializing in online learning and video production. For outstanding work in Miami Call: 415 408 8549."

    print("📝 Company Name:", company_name)
    print("🏭 Industry Info:", industry_info)
    print("📦 Offer Summary:", offer_summary)

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
7. Be specific with industry context: instead of “businesses like yours,” say “other {industry_info.lower()} businesses.”
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

    print("🧪 Prompt being sent to OpenAI:\n", prompt)

    print("\n🔄 Generating response from OpenAI...\n")
    openai_key = load_openai_key()
    print("🔑 OpenAI Key loaded successfully.")
    email = generate_email_from_prompt(prompt, openai_key)
    print("✅ Email generation completed.")

    print("📧 Generated Email:\n")
    print(email)
    print("\n✉️ Plaintext Output:\n")
    if isinstance(email, dict):
        print(email.get("body_text", "[No plain text found in email dictionary]"))
    else:
        print(email)

if __name__ == "__main__":
    run_prompt_test()