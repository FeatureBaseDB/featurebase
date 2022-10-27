import SignOutButton from "App/AuthFlow/SignOutButton";
import { ReactComponent as FeatureBaseLogoDark } from "assets/darkTheme/featurebase-logo.svg";
import { ReactComponent as FeatureBaseLogo } from "assets/lightTheme/featurebase-logo.svg";
import { FC } from "react";
import { Link } from "react-router-dom";
import { useAuth } from "services/useAuth";
import { ThemeToggle } from "shared/ThemeToggle";

import AppBar from "@material-ui/core/AppBar";
import Button from "@material-ui/core/Button";
import { useTheme } from "@material-ui/core/styles";
import Toolbar from "@material-ui/core/Toolbar";

import css from "./Header.module.scss";

type HeaderProps = {
  onToggleTheme: () => void;
};

export const Header: FC<HeaderProps> = ({ onToggleTheme }) => {
  const theme = useTheme();
  const isDark = theme.palette.type === "dark";
  const auth = useAuth();

  return (
    <AppBar
      className={css.appToolbar}
      position="static"
      color="default"
      elevation={0}
    >
      <Toolbar>
        <div className={css.toolbarLayout}>
          <Link to="/" className={css.homeLink}>
            {!isDark && <FeatureBaseLogo className={css.logo} />}
            {isDark && <FeatureBaseLogoDark className={css.logo} />}
          </Link>

          <div className={css.toolbarActions}>
            <div className={css.toggle}>
              <ThemeToggle
                theme={theme.palette.type}
                onToggle={onToggleTheme}
              />
            </div>
          </div>

          {auth.isAuthenticated ? (
            <div>
              {auth.user && (
                <Button
                  style={{
                    backgroundColor: "transparent",
                    pointerEvents: "none",
                  }}
                >
                  {auth.user.username}
                </Button>
              )}
              <SignOutButton />
            </div>
          ) : null}
        </div>
      </Toolbar>
    </AppBar>
  );
};
