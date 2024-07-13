from typing import List, Optional, Literal
from pydantic import BaseModel, Field


class JobPosting(BaseModel):
    company_name: str
    positions: List[str]
    location: str
    job_type: Literal["Full Time", "Part Time", "Contractor", "Unknown"]
    salary_range: Optional[str] = None
    benefits: List[str] = Field(default_factory=list)
    required_skills: List[str] = Field(default_factory=list)
    job_description: str
    additional_requirements: List[str] = Field(default_factory=list)
    work_environment: Optional[str] = None
    application_instructions: Optional[str] = None
    is_remote: bool = False
    is_remote_in_us: bool = False
    is_remote_global: bool = False
    timezone: Optional[str] = None
    industry: Optional[str] = None
    startup_series: Literal[
        "Series A",
        "Series B",
        "Series C",
        "Series D",
        "Series E",
        "Public Company",
        "Unknown",
    ] = "Unknown"
    is_ml: bool = False
    is_datacenter: bool = False
    year_of_experience: Optional[str] = None
