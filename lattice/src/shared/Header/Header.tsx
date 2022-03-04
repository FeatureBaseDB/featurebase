import SignOutButton from "App/AuthFlow/SignOutButton";
import { ReactComponent as MoleculaLogoDark } from "assets/darkTheme/MoleculaLogo.svg";
import { ReactComponent as MoleculaLogo } from "assets/lightTheme/MoleculaLogo.svg";
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
            {!isDark && <MoleculaLogo className={css.logo} />}
            {isDark && <MoleculaLogoDark className={css.logo} />}
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
