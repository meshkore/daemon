/**
 * `meshcore agent create` — declare a new agent identity in
 * .meshkore/agents/<identity>.yaml.
 *
 * `meshcore agent list` — show all identities declared.
 */
interface AgentCreateOptions {
  client: 'claude-code' | 'deepseek' | 'qwen' | 'cursor' | 'custom';
  identity: string;
  role: string;
}

export const agentCmd = {
  async create(_opts: AgentCreateOptions): Promise<void> {
    // TODO: implement
    //  - prompt for credentials (API key); save to
    //    .meshkore/credentials/<client>.env
    //  - write .meshkore/agents/<identity>.yaml
    console.log('TODO: implementation pending — see task C1 in roadmap.');
  },
  async list(): Promise<void> {
    // TODO: read .meshkore/agents/*.yaml, print table
    console.log('TODO: implementation pending — see task C1 in roadmap.');
  },
};
