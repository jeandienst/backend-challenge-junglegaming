import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';

/** Ponto de extensão explícito: substituir por validação OIDC no IdP escolhido. */
@Injectable()
export class NoopAuthGuard implements CanActivate {
  canActivate(_context:ExecutionContext):boolean{return true;}
}
