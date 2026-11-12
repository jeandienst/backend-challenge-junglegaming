export interface ProviderIdentityPort {
  validate(providerId:string, credential?:string):Promise<boolean>;
}
export const PROVIDER_IDENTITY_PORT='PROVIDER_IDENTITY_PORT';
