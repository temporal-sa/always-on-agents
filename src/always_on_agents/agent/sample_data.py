"""Sample inputs standing in for the user's document store and the web."""

from __future__ import annotations

DOCUMENTS = [
    {
        "id": "doc-hiring",
        "kind": "decision",
        "title": "Approve the Q1 hiring plan",
        "body": "Finance needs a yes or no on four new platform roles by Friday.",
    },
    {
        "id": "doc-vendor",
        "kind": "decision",
        "title": "Pick a warehouse migration vendor",
        "body": "Two bids remain. Procurement asks for a choice before the contract window closes on the 30th.",
    },
    {
        "id": "doc-launch",
        "kind": "update",
        "title": "Launch metrics",
        "body": "Weekly active teams rose 8% after the onboarding redesign; activation is flat.",
    },
    {
        "id": "doc-incident",
        "kind": "update",
        "title": "Incident review",
        "body": "Tuesday's 40-minute API outage traced to an expired certificate; rotation is now automated.",
    },
]


def research_sources(question: str) -> list[dict[str, str]]:
    return [
        {
            "title": "Analyst note",
            "snippet": f"Two competitors cut entry-tier prices about 10% this week ({question[:40]}...).",
        },
        {
            "title": "Public pricing pages",
            "snippet": "Enterprise tiers were unchanged across the three largest vendors.",
        },
    ]
