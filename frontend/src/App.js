import React from 'react';
import { QueryClient, QueryClientProvider } from 'react-query';
import Dashboard from './components/Dashboard';
import './App.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 3
    }
  }
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <div className="app">
        <Dashboard />
      </div>
    </QueryClientProvider>
  );
}

export default App;
