let activeEventSession: string | null = null;

export function setActiveEventSession(session: string | null) {
  activeEventSession = session;
}

export function acceptsEventSession(envelope: any): boolean {
  return activeEventSession !== null && envelope?.eventSession === activeEventSession;
}
