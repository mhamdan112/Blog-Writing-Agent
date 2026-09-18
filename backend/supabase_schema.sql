create table public.blogs (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  content text not null,
  created_at timestamptz not null default now()
);

alter table public.blogs enable row level security;

create policy "service role can manage blogs"
  on public.blogs
  for all
  using (auth.role() = 'service_role')
  with check (auth.role() = 'service_role');