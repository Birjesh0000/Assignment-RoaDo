/**
 * Custom Hooks
 * ============
 * React Query hooks for data fetching
 */

import { useQuery } from 'react-query';
import { analyticsApi } from '../services/api';

export const useSessionsByTier = (daysBack = 90) => {
  return useQuery(
    ['sessions-by-tier', daysBack],
    () => analyticsApi.getSessionsByTier(daysBack).then(res => res.data.data),
    {
      staleTime: 5 * 60 * 1000, // 5 minutes
      refetchInterval: 15 * 60 * 1000, // 15 minutes
      retry: 3
    }
  );
};

export const useFeatureDauRetention = (daysBack = 30, limit = 50) => {
  return useQuery(
    ['feature-dau-retention', daysBack, limit],
    () => analyticsApi.getFeatureDauRetention(daysBack, limit).then(res => res.data.data),
    {
      staleTime: 5 * 60 * 1000,
      refetchInterval: 15 * 60 * 1000,
      retry: 3
    }
  );
};

export const useOnboardingFunnel = () => {
  return useQuery(
    ['onboarding-funnel'],
    () => analyticsApi.getOnboardingFunnel().then(res => res.data.data),
    {
      staleTime: 10 * 60 * 1000,
      refetchInterval: 30 * 60 * 1000,
      retry: 3
    }
  );
};

export const useEngagedFreeUsers = (limit = 20, daysBack = 90) => {
  return useQuery(
    ['engaged-free-users', limit, daysBack],
    () => analyticsApi.getEngagedFreeUsers(limit, daysBack).then(res => res.data.data),
    {
      staleTime: 5 * 60 * 1000,
      refetchInterval: 15 * 60 * 1000,
      retry: 3
    }
  );
};

export default {
  useSessionsByTier,
  useFeatureDauRetention,
  useOnboardingFunnel,
  useEngagedFreeUsers
};
