"use client";

import { Component, type ReactNode } from "react";

type GlobalErrorBoundaryProps = {
  children: ReactNode;
};

type GlobalErrorBoundaryState = {
  hasError: boolean;
};

export class GlobalErrorBoundary extends Component<
  GlobalErrorBoundaryProps,
  GlobalErrorBoundaryState
> {
  public constructor(props: GlobalErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  public static getDerivedStateFromError(): GlobalErrorBoundaryState {
    return { hasError: true };
  }

  private readonly handleReset = (): void => {
    this.setState({ hasError: false });
  };

  public render(): ReactNode {
    if (this.state.hasError) {
      return (
        <section>
          <h2>Erro inesperado na plataforma</h2>
          <p>TODO: fallback global de erro.</p>
          <button onClick={this.handleReset} type="button">
            Tentar novamente
          </button>
        </section>
      );
    }

    return this.props.children;
  }
}
