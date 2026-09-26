import React, { useState } from 'react';

interface PlanFeature {
  label: string;
  type: 'included' | 'addon' | 'excluded' | 'general';
  addonPrice?: string;
  specs?: { cpu?: string; mem?: string };
}

interface Plan {
  name: string;
  description: string;
  monthlyPrice: number;
  yearlyPrice: number;
  command: string | null;
  ctaHref?: string;
  popular: boolean;
  features: PlanFeature[];
}

const plans: Plan[] = [
  {
    name: 'Starter',
    description: 'For side projects and small apps.',
    monthlyPrice: 29,
    yearlyPrice: 290,
    command: 'npx @spky/cli deploy --plan starter',
    popular: false,
    features: [
      { label: 'Scheduler', type: 'included', specs: { cpu: '1', mem: '512MB' } },
      { label: '1 SSP instance', type: 'included', specs: { cpu: '1', mem: '512MB' } },
      { label: '1 Backend Service', type: 'included', specs: { cpu: '1', mem: '512MB' } },
      { label: '5GB file storage', type: 'included' },
      { label: 'CI/CD Cloud Builder (60 min/mo)', type: 'included' },
      { label: 'Shared Vault', type: 'included' },
      { label: 'Custom domains', type: 'included' },
      { label: 'Community support', type: 'included' },
      { label: 'SSO', type: 'excluded' },
      { label: 'Backups', type: 'excluded' },
      { label: '5 team members included', type: 'excluded' },
      { label: 'Priority support', type: 'excluded' },
    ],
  },
  {
    name: 'Pro',
    description: 'For teams shipping production apps.',
    monthlyPrice: 229,
    yearlyPrice: 2290,
    command: 'npx @spky/cli deploy --plan pro',
    popular: true,
    features: [
      { label: 'Scheduler', type: 'included', specs: { cpu: '2', mem: '2GB' } },
      { label: '3 SSP instances', type: 'included', specs: { cpu: '2', mem: '1GB' } },
      { label: '1 Backend Service', type: 'included', specs: { cpu: '2', mem: '1GB' } },
      { label: '20GB file storage', type: 'included' },
      { label: 'CI/CD Cloud Builder (600 min/mo)', type: 'included' },
      { label: 'Shared Vault', type: 'included' },
      { label: 'Custom domains', type: 'included' },
      { label: 'Community support', type: 'included' },
      { label: 'SSO', type: 'included' },
      { label: 'Unlimited backups (50GB)', type: 'included' },
      { label: '5 additional team members', type: 'included' },
      { label: 'Priority support', type: 'included' },
    ],
  },
  {
    name: 'Self-hosted',
    description: 'Your infrastructure, no limits.',
    monthlyPrice: 0,
    yearlyPrice: 0,
    command: null,
    ctaHref: 'https://github.com/mono424/sp00ky',
    popular: false,
    features: [
      { label: 'Full source access', type: 'general' },
      { label: 'Unlimited everything', type: 'general' },
      { label: 'Community support', type: 'general' },
      { label: 'Self-managed infrastructure', type: 'general' },
    ],
  },
];

function Tooltip({ label, children }: { label: string; children: React.ReactNode }) {
  const [visible, setVisible] = useState(false);
  return (
    <span
      className="relative inline-flex"
      onMouseEnter={() => setVisible(true)}
      onMouseLeave={() => setVisible(false)}
    >
      {children}
      <span
        className={`absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 px-2 py-1 text-[11px] font-medium text-text-primary bg-white/[0.08] backdrop-blur-sm border border-white/[0.08] rounded-md whitespace-nowrap pointer-events-none transition-all duration-150 ${
          visible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-1'
        }`}
      >
        {label}
      </span>
    </span>
  );
}

function SpecBadges({ specs }: { specs: { cpu?: string; mem?: string } }) {
  return (
    <span className="inline-flex items-center gap-1 ml-auto shrink-0">
      {specs.cpu && (
        <Tooltip label={`${specs.cpu} vCPU`}>
          <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-white/[0.04] text-[10px] font-mono tabular-nums text-text-muted leading-none">
            <svg className="w-2.5 h-2.5 opacity-50" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 3v1.5M4.5 8.25H3m18 0h-1.5M4.5 12H3m18 0h-1.5M4.5 15.75H3m18 0h-1.5M8.25 19.5V21M12 3v1.5m0 15V21m3.75-18v1.5m0 15V21m-9-1.5h9a2.25 2.25 0 002.25-2.25v-9a2.25 2.25 0 00-2.25-2.25h-9A2.25 2.25 0 006 8.25v9a2.25 2.25 0 002.25 2.25z" />
            </svg>
            {specs.cpu}
          </span>
        </Tooltip>
      )}
      {specs.mem && (
        <Tooltip label={`${specs.mem} Memory`}>
          <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-white/[0.04] text-[10px] font-mono tabular-nums text-text-muted leading-none">
            <svg className="w-2.5 h-2.5 opacity-50" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 3h12a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V5a2 2 0 012-2zm0 8h12a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2a2 2 0 012-2z" />
            </svg>
            {specs.mem}
          </span>
        </Tooltip>
      )}
    </span>
  );
}

function formatPrice(price: number): string {
  if (price === 0) return 'Free';
  return `$${price.toLocaleString('en-US')}`;
}

function CopyCommand({ command }: { command: string }) {
  const [copied, setCopied] = useState(false);

  const copy = async () => {
    await navigator.clipboard.writeText(command);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <button
      onClick={copy}
      className="flex items-center gap-2 w-full px-3 py-2 rounded-lg bg-white/[0.03] border border-white/[0.06] hover:border-white/[0.12] transition-all duration-200 group text-left"
    >
      <span className="text-text-muted select-none">$</span>
      <code className="text-[12px] font-mono text-text-tertiary flex-1 truncate">{command}</code>
      {copied ? (
        <svg className="w-3.5 h-3.5 text-accent-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
        </svg>
      ) : (
        <svg className="w-3.5 h-3.5 text-text-muted group-hover:text-text-tertiary shrink-0 transition-colors" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
        </svg>
      )}
    </button>
  );
}

export const PricingPage: React.FC = () => {
  const [yearly, setYearly] = useState(false);

  return (
    <div className="max-w-5xl mx-auto px-4">
      {/* Toggle */}
      <div className="flex justify-center mb-12">
        <div className="relative inline-grid grid-cols-2 rounded-full bg-white/[0.03] border border-white/[0.06] p-1">
          <button
            onClick={() => setYearly(false)}
            className={`relative z-10 px-5 py-1.5 text-[13px] font-medium rounded-full transition-colors duration-200 text-center ${
              !yearly ? 'text-text-primary' : 'text-text-muted hover:text-text-tertiary'
            }`}
          >
            Monthly
          </button>
          <button
            onClick={() => setYearly(true)}
            className={`relative z-10 px-5 py-1.5 text-[13px] font-medium rounded-full transition-colors duration-200 text-center ${
              yearly ? 'text-text-primary' : 'text-text-muted hover:text-text-tertiary'
            }`}
          >
            Yearly
          </button>
          {/* Sliding background */}
          <div
            className="absolute top-1 bottom-1 rounded-full bg-white/[0.07] transition-all duration-300 ease-[cubic-bezier(0.4,0,0.2,1)]"
            style={{
              width: 'calc(50% - 4px)',
              left: yearly ? 'calc(50% + 2px)' : '4px',
            }}
          />
        </div>
      </div>

      {/* Plan Cards */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {plans.map((plan) => {
          const price = yearly ? plan.yearlyPrice : plan.monthlyPrice;
          const period = plan.monthlyPrice === 0 ? null : yearly ? '/yr' : '/mo';

          return (
            <div
              key={plan.name}
              className={`relative flex flex-col rounded-xl p-6 ${
                plan.popular
                  ? 'bg-white/[0.04] border border-white/[0.1]'
                  : 'bg-white/[0.02] border border-white/[0.06]'
              }`}
            >
              {/* Header */}
              <div className="flex items-center gap-2 mb-1">
                <h3 className="text-[15px] font-semibold text-text-primary">{plan.name}</h3>
                {plan.popular && (
                  <span className="text-[10px] font-medium text-text-tertiary bg-white/[0.06] px-2 py-0.5 rounded-full uppercase tracking-wide">
                    Popular
                  </span>
                )}
              </div>
              <p className="text-[13px] text-text-muted mb-5">{plan.description}</p>

              {/* Price */}
              <div className="mb-5">
                <span className="text-3xl font-semibold text-text-primary tracking-tight">
                  {formatPrice(price)}
                </span>
                {period && (
                  <span className="text-[13px] text-text-muted ml-1">{period}</span>
                )}
                {price === 0 && (
                  <span className="text-[13px] text-text-muted ml-1">forever</span>
                )}
              </div>

              {/* CTA */}
              {plan.command ? (
                <CopyCommand command={plan.command} />
              ) : (
                <a
                  href={plan.ctaHref}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="block w-full text-center px-4 py-2 text-[13px] font-medium rounded-lg border border-white/[0.08] text-text-tertiary hover:text-text-primary hover:border-white/[0.15] transition-all duration-200"
                >
                  View on GitHub
                </a>
              )}

              {/* Features */}
              <div className="border-t border-white/[0.06] mt-5 pt-5">
                <ul className="space-y-2.5">
                  {plan.features.map((feature, i) => (
                    <React.Fragment key={feature.label}>
                      {feature.type === 'addon' && i > 0 && plan.features[i - 1].type !== 'addon' && (
                        <li className="border-t border-white/[0.04] my-2" />
                      )}
                      <li className={`flex items-center gap-2.5 text-[13px] ${
                        feature.type === 'excluded' ? 'text-text-quaternary' : 'text-text-tertiary'
                      }`}>
                        {feature.type === 'excluded' ? (
                          <svg
                            className="w-3.5 h-3.5 text-text-quaternary shrink-0"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                            strokeWidth={2}
                          >
                            <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                          </svg>
                        ) : feature.type === 'addon' ? (
                          <svg
                            className="w-3.5 h-3.5 text-text-quaternary shrink-0"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                            strokeWidth={2}
                          >
                            <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m8-8H4" />
                          </svg>
                        ) : (
                          <svg
                            className="w-3.5 h-3.5 text-text-muted shrink-0"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                            strokeWidth={2}
                          >
                            <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                          </svg>
                        )}
                        <span className={feature.type === 'excluded' ? 'line-through' : ''}>{feature.label}</span>
                        {feature.specs && <SpecBadges specs={feature.specs} />}
                        {feature.addonPrice && (
                          <span className="text-text-muted ml-auto text-[12px]">{feature.addonPrice}</span>
                        )}
                      </li>
                    </React.Fragment>
                  ))}
                </ul>
              </div>
            </div>
          );
        })}
      </div>

      {/* Add-ons */}
      <div className="mt-20 max-w-xl mx-auto">
        <p className="text-[13px] text-text-muted mb-6">Add-ons</p>
        <div className="space-y-0">
          {[
            { name: 'SurrealDB', price: 'from $19/mo' },
            { name: 'Team member', price: '$5/mo' },
            { name: 'SSP instance', price: '$15/mo' },
            { name: 'Backend service', price: '$10/mo' },
            { name: 'Extra file storage', price: '$0.10/GB/mo' },
            { name: 'Backup storage', price: '$0.50/GB/mo' },
            { name: 'Build minutes', price: '$0.07/min' },
          ].map((addon) => (
            <div
              key={addon.name}
              className="flex items-center justify-between py-3 border-b border-white/[0.06]"
            >
              <span className="text-[13px] text-text-tertiary">{addon.name}</span>
              <span className="text-[13px] text-text-muted tabular-nums">{addon.price}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};
