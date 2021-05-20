import React from 'react';
import { motion } from 'framer-motion';

const groupVariants = {
  initial: {
    transition: {
      when: 'afterChildren',
      staggerChildren: 0.05,
      staggerDirection: -1 as any
    }
  },
  animate: {
    transition: {
      when: 'beforeChildren',
      staggerChildren: 0.03,
      delayChildren: 0.3
    }
  }
};

const fadeVariants = {
  initial: {
    scale: 0.5,
    opacity: 0,
    transition: {
      scale: { stiffness: 100 }
    }
  },
  animate: {
    scale: 1,
    opacity: 1,
    transition: {
      scale: { stiffness: 100, velocity: -100 }
    }
  }
};

const fadeExit = {
  scale: 0,
  opacity: 0,
  transition: {
    scale: { stiffness: 100, velocity: -100 }
  }
};

const dropInVariants = {
  initial: {
    y: -20,
    opacity: 0,
    transition: {
      y: { stiffness: 100 }
    }
  },
  animate: {
    y: 0,
    opacity: 1,
    transition: {
      y: { stiffness: 100, velocity: -100 }
    }
  }
};

const slideInVariants = {
  initial: {
    x: -20,
    opacity: 0,
    transition: {
      x: { stiffness: 100 }
    }
  },
  animate: {
    x: 0,
    opacity: 1,
    transition: {
      x: { stiffness: 100, velocity: -100 },
      opacity: { duration: 1 }
    }
  }
};

export const MotionGroup = ({ children, ...rest }) => (
  <motion.div
    variants={groupVariants}
    initial="initial"
    animate="animate"
    {...rest}
  >
    {children}
  </motion.div>
);

export const MotionHeader = ({ children, ...rest }) => (
  <motion.div variants={dropInVariants} {...rest}>
    {children}
  </motion.div>
);

export const MotionFadeItem = ({ children, ...rest }) => (
  <motion.div variants={fadeVariants} exit={fadeExit} {...rest}>
    {children}
  </motion.div>
);

export const MotionSlideItem = ({ children, ...rest }) => (
  <motion.div variants={slideInVariants} {...rest}>
    {children}
  </motion.div>
);

export const MotionGrowItem = ({ children, ...rest }) => (
  <motion.div
    initial={{
      height: 0,
      opacity: 0
    }}
    animate={{
      height: '100%',
      opacity: 1,
      transition: {
        height: { stiffness: 100, velocity: -100 },
        opacity: { duration: 1 }
      }
    }}
    exit={{
      height: 0,
      opacity: 0,
      transition: {
        height: { stiffness: 100, velocity: -100 }
      }
    }}
    {...rest}
  >
    {children}
  </motion.div>
);
