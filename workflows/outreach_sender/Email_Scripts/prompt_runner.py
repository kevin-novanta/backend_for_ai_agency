import sys
import os
# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


import json
from pathlib import Path
from workflows.outreach_sender.AI_Intergrations.opener_ai_writer import generate_email_from_prompt

def load_openai_key():
    creds_path = "/Users/kevinnovanta/backend_for_ai_agency/Creds/gpt_key.json"
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
Write a short, warm cold outreach email (under 110 words) inviting the company to a quick discovery call (free workflow audit) where we identify bottlenecks and propose a tailored automation plan.

### Script Rules:
1. Do NOT re-explain what they do; show you understand their offer by referencing it naturally.
2. Mention our company — Outbound Accelerator — as specialists in advanced AI workflows and ops automation.
3. Use this context line once: "We help other {industry_info.lower()} businesses streamline messy processes, cut manual work, and increase booked calls without adding headcount."
4. Touch 1–2 pains relevant to {offer_summary.lower()}:
   - Human error from multi-tool chaos
   - No time to follow up properly
   - Leads slipping through the cracks
   - Manual, repetitive tasks blocking growth
5. Mention 1–2 practical outcomes:
   - Clear workflow map in 7 days
   - Automated follow-ups
   - Cleaner CRM and reporting
6. Clear CTA: invite them to a 15–20 min discovery call (free) to map quick wins; offer to send a 1‑page blueprint if they prefer async.
7. Keep tone human, specific, concise. No hype, no emojis, no square brackets.
8. Final sentence must include our company name: Outbound Accelerator.

### Output Format:
- Casual, friendly, human
- 1–2 short paragraphs + a one‑line CTA
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