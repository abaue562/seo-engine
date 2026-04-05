"use client";

import { useState, useEffect } from "react";

export interface BusinessData extends Record<string, unknown> {
  business_name: string;
  website: string;
  gbp_url: string;
  primary_service: string;
  secondary_services: string[];
  primary_city: string;
  service_areas: string[];
  primary_keywords: string[];
  competitors: string[];
  reviews_count: number;
  rating: number;
  years_active: number;
  target_customer: string;
  avg_job_value: number;
  monthly_traffic: number;
  gbp_views: number;
  current_rankings: Record<string, number>;
  missing_keywords: string[];
}

const EMPTY: BusinessData = {
  business_name: "", website: "", gbp_url: "", primary_service: "",
  secondary_services: [], primary_city: "", service_areas: [],
  primary_keywords: [], competitors: [], reviews_count: 0, rating: 0,
  years_active: 0, target_customer: "", avg_job_value: 0,
  monthly_traffic: 0, gbp_views: 0, current_rankings: {}, missing_keywords: [],
};

const DEMO: BusinessData = {
  business_name: "Demo Plumbing Co",
  website: "https://demoplumbing.com",
  gbp_url: "",
  primary_service: "Plumbing",
  secondary_services: ["Drain Cleaning", "Water Heater Repair"],
  primary_city: "Austin",
  service_areas: ["Austin", "Round Rock", "Cedar Park"],
  primary_keywords: ["plumber austin", "emergency plumber austin", "drain cleaning austin"],
  competitors: ["ABC Plumbing", "Pro Drain Solutions", "Austin Rooter"],
  reviews_count: 127, rating: 4.7, years_active: 8,
  target_customer: "Homeowners", avg_job_value: 350,
  monthly_traffic: 2400, gbp_views: 8500,
  current_rankings: { "plumber austin": 11, "emergency plumber austin": 15 },
  missing_keywords: ["water heater repair austin", "24 hour plumber austin"],
};

function listToStr(arr: string[]): string {
  return arr.join(", ");
}
function strToList(s: string): string[] {
  return s.split(",").map((v) => v.trim()).filter(Boolean);
}

export default function BusinessInput({
  onSubmit,
  loading,
}: {
  onSubmit: (business: BusinessData) => void;
  loading: boolean;
}) {
  const [expanded, setExpanded] = useState(false);

  // Simple fields stored directly
  const [name, setName] = useState(DEMO.business_name);
  const [website, setWebsite] = useState(DEMO.website);
  const [service, setService] = useState(DEMO.primary_service);
  const [city, setCity] = useState(DEMO.primary_city);
  const [gbpUrl, setGbpUrl] = useState(DEMO.gbp_url);
  const [customer, setCustomer] = useState(DEMO.target_customer);
  const [reviews, setReviews] = useState(DEMO.reviews_count);
  const [rating, setRating] = useState(DEMO.rating);
  const [years, setYears] = useState(DEMO.years_active);
  const [traffic, setTraffic] = useState(DEMO.monthly_traffic);
  const [jobValue, setJobValue] = useState(DEMO.avg_job_value);

  // List fields stored as RAW TEXT — only parsed on submit
  const [keywordsText, setKeywordsText] = useState(listToStr(DEMO.primary_keywords));
  const [competitorsText, setCompetitorsText] = useState(listToStr(DEMO.competitors));
  const [secondaryText, setSecondaryText] = useState(listToStr(DEMO.secondary_services));
  const [areasText, setAreasText] = useState(listToStr(DEMO.service_areas));

  const loadDemo = () => {
    setName(DEMO.business_name); setWebsite(DEMO.website); setService(DEMO.primary_service);
    setCity(DEMO.primary_city); setGbpUrl(DEMO.gbp_url); setCustomer(DEMO.target_customer);
    setReviews(DEMO.reviews_count); setRating(DEMO.rating); setYears(DEMO.years_active);
    setTraffic(DEMO.monthly_traffic); setJobValue(DEMO.avg_job_value);
    setKeywordsText(listToStr(DEMO.primary_keywords));
    setCompetitorsText(listToStr(DEMO.competitors));
    setSecondaryText(listToStr(DEMO.secondary_services));
    setAreasText(listToStr(DEMO.service_areas));
  };

  const clearAll = () => {
    setName(""); setWebsite(""); setService(""); setCity(""); setGbpUrl("");
    setCustomer(""); setReviews(0); setRating(0); setYears(0); setTraffic(0); setJobValue(0);
    setKeywordsText(""); setCompetitorsText(""); setSecondaryText(""); setAreasText("");
  };

  const handleSubmit = () => {
    const biz: BusinessData = {
      business_name: name,
      website: website,
      gbp_url: gbpUrl,
      primary_service: service,
      secondary_services: strToList(secondaryText),
      primary_city: city,
      service_areas: strToList(areasText),
      primary_keywords: strToList(keywordsText),
      competitors: strToList(competitorsText),
      reviews_count: reviews,
      rating: rating,
      years_active: years,
      target_customer: customer,
      avg_job_value: jobValue,
      monthly_traffic: traffic,
      gbp_views: 0,
      current_rankings: {},
      missing_keywords: [],
    };
    onSubmit(biz);
  };

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-bold">Business Setup</h2>
        <div className="flex gap-2">
          <button className="btn btn-ghost text-xs" onClick={loadDemo}>Load Demo</button>
          <button className="btn btn-ghost text-xs" onClick={clearAll}>Clear</button>
          <button className="btn btn-ghost text-xs" onClick={() => setExpanded(!expanded)}>
            {expanded ? "Simple" : "Advanced"}
          </button>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <input className="input" placeholder="Business Name *" value={name} onChange={(e) => setName(e.target.value)} />
        <input className="input" placeholder="Website (https://...)" value={website} onChange={(e) => setWebsite(e.target.value)} />
        <input className="input" placeholder="Primary Service *" value={service} onChange={(e) => setService(e.target.value)} />
        <input className="input" placeholder="Primary City *" value={city} onChange={(e) => setCity(e.target.value)} />
        <input
          className="input col-span-2"
          placeholder="Target Keywords (comma separated, e.g. plumber austin, emergency plumber)"
          value={keywordsText}
          onChange={(e) => setKeywordsText(e.target.value)}
        />
        <input
          className="input col-span-2"
          placeholder="Competitors (comma separated)"
          value={competitorsText}
          onChange={(e) => setCompetitorsText(e.target.value)}
        />
      </div>

      {expanded && (
        <div className="grid grid-cols-2 gap-3 mt-3 pt-3" style={{ borderTop: "1px solid var(--border)" }}>
          <input className="input" placeholder="GBP URL" value={gbpUrl} onChange={(e) => setGbpUrl(e.target.value)} />
          <input
            className="input"
            placeholder="Secondary Services (comma separated)"
            value={secondaryText}
            onChange={(e) => setSecondaryText(e.target.value)}
          />
          <input
            className="input"
            placeholder="Service Areas (comma separated)"
            value={areasText}
            onChange={(e) => setAreasText(e.target.value)}
          />
          <input className="input" placeholder="Target Customer" value={customer} onChange={(e) => setCustomer(e.target.value)} />
          <input className="input" type="number" placeholder="Reviews Count" value={reviews || ""} onChange={(e) => setReviews(Number(e.target.value))} />
          <input className="input" type="number" step="0.1" placeholder="Rating (e.g. 4.7)" value={rating || ""} onChange={(e) => setRating(Number(e.target.value))} />
          <input className="input" type="number" placeholder="Monthly Traffic" value={traffic || ""} onChange={(e) => setTraffic(Number(e.target.value))} />
          <input className="input" type="number" placeholder="Avg Job Value ($)" value={jobValue || ""} onChange={(e) => setJobValue(Number(e.target.value))} />
          <input className="input" type="number" placeholder="Years Active" value={years || ""} onChange={(e) => setYears(Number(e.target.value))} />
        </div>
      )}

      <button
        className="btn btn-primary w-full mt-4"
        onClick={handleSubmit}
        disabled={loading || !name || !service || !city}
      >
        {loading ? "Running System..." : "Run SEO System"}
      </button>
    </div>
  );
}
